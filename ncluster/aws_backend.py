"""AWS implementation of backend.py

Not thread-safe
"""
import glob
import os
import pprint
import shlex
import signal
import stat
import sys
import threading
import time
from typing import Tuple, List, Optional, Union

import getpass

import paramiko
import portalocker
from boto3_type_annotations.ec2 import Instance

from ncluster import ncluster_globals

from . import ncluster_cloud_setup as create_lib
from . import aws_util as u
from . import util

# costs from https://aws.amazon.com/ec2/pricing/on-demand/
_RAW_INSTANCE_INFO = [
    ['p3.2xlarge', 3.06, 1, 16],
    ['p3.8xlarge', 12.24, 4, 16],
    ['p3.16xlarge', 24.48, 8, 16],
    ['p3dn.24xlarge', 31.212, 8, 32],
    ['c5.18xlarge', 3.06, None, None],
    ['c5n.18xlarge', 3.888, None, None],
    ['c5d.9xlarge', 1.728, None, None],
    ['c5d.18xlarge', 3.456, None, None],
    ['m4.2xlarge', 0.40, None, None],
    ['r5.large', 0.126, None, None],
    ['t2.nano', 0.005, None, None],
]
_RAW_HEADERS = ['cost', 'gpus', 'gpu_mem_gb']

INSTANCE_INFO = dict([(x[0], dict(zip(_RAW_HEADERS, x[1:]))) for x in _RAW_INSTANCE_INFO])

TMPDIR = '/tmp/ncluster'  # location for temp files on launching machine
AWS_LOCK_FN = '/tmp/aws.lock'  # lock file used to prevent concurrent creation of AWS resources by multiple workers in parallel
NCLUSTER_DEFAULT_REGION = 'us-east-1'  # used as last resort if no other method set a region

# default value of logdir root for this backend (can override with set_logdir_root)
LOGDIR_ROOT = '/ncluster/runs'

# some image which is fast to load, to use for quick runs

# Default image/instance types to use for make_task() with no arguments
# get id from https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#LaunchInstanceWizard:
# then "ncluster lookup_image_name  ami-0cc96feef8c6bbff3"
GENERIC_SMALL_IMAGE = 'amzn2-ami-hvm-2.0.20190612-x86_64-gp2'
GENERIC_SMALL_INSTANCE = 't3.micro'

# Uncomment when doing line profiling
# def identity(x):
#   return x
#
#
# try:
#   profile  # throws an exception when profile isn't defined
# except NameError:
#   profile = identity   # if it's not defined simply ignore the decorator.


# def check_cmd(cmd):

def get_logdir_root() -> str:
  return LOGDIR_ROOT


def set_logdir_root(logdir_root):
  """Globally changes logdir root for all runs."""
  global LOGDIR_ROOT
  LOGDIR_ROOT = logdir_root


class DummyTask:
  name: str

  def __init__(self, name):
    self.name = name

  def log(self, message, *args):
    """Log to launcher console."""
    if args:
      message %= args

    print(f"{util.current_timestamp()} {self.name}: {message}")


class Task:
  """AWS task is initialized with an AWS instance and handles initialization,
  creation of SSH session, shutdown"""
  last_status: int  # status of last command executed

  tmux_window_id: int
  tmux_available_window_ids: List[int]
  instance: Instance

  sftp: Optional[paramiko.SFTPClient]

  # TODO(y): init should be lightweight to enable wrapping existing instances into tasks, refactor all setup to take place outside of init
  # TODO(y): clean-up interface
  def __init__(self, name, *, instance: Instance, install_script='', image_name='',
               instance_reuse=False, **extra_kwargs):
    """
   Initializes Task on top of existing AWS instance. Blocks until instance is ready to execute
   shell commands.

    Args:
      name: task name
      instance: ec2.Instance object (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#instance)
      install_script:
      image_name: AWS image name
      **extra_kwargs: unused kwargs (kept for compatibility with other backends)
    """

    # self._cmd_fn = None
    self._cmd = None
    self._status_fn = None  # location of output of last status
    self._out_fn = None
    self.last_status = -1

    self._can_run = False  # indicates that things needed for .run were created
    self.initialize_called = False

    self.name = name
    self.instance = instance
    self.install_script = install_script
    self.extra_kwargs = extra_kwargs
    self.instance_reuse = instance_reuse

    self.public_ip = u.get_public_ip(instance)
    self.ip = u.get_ip(instance)
    self.sftp = None
    self._linux_type = 'ubuntu'

    # TODO(y): this logic is duplicated in aws_util, reuse that function instead
    # heuristic to tell if I'm using Amazon image name
    # default image has name like 'amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2'
    if u.is_amazon_ami(image_name):
      self.log('Detected Amazon Linux image')
      self._linux_type = 'amazon'
    self.run_counter = 0

    launch_id = util.random_id()
    self.local_scratch = f"{TMPDIR}/{name}-{launch_id}"
    self.remote_scratch = f"{TMPDIR}/{name}-{launch_id}"

    os.system('mkdir -p ' + self.local_scratch)

    # _current_directory tracks current directory on task machine
    # used for uploading without specifying absolute path on target machine
    manual_username = util.get_env('NCLUSTER_SSH_USERNAME')
    if manual_username:
      self.log(f"manual SSH username '{manual_username}' specified")
      self.ssh_username = manual_username
      if manual_username == 'ec2-user':
        self._linux_type = 'amazon'
    else:
      if self._linux_type == 'ubuntu':
        #      self._current_directory = '/home/ubuntu'
        self.ssh_username = 'ubuntu'  # default username on task machine
      elif self._linux_type == 'amazon':
        #      self._current_directory = '/home/ec2-user'
        self.ssh_username = 'ec2-user'
      self.log(f"Autodetecting username '{self.ssh_username}'")

    self.homedir = '/home/' + self.ssh_username

    self.ssh_client = u.ssh_to_task(self)
    self._setup_tmux()

    # can't skip this setup because remote_scratch location changes each rerun
    self._run_raw('mkdir -p ' + self.remote_scratch)

    self._can_run = True

    # Macine setup has two parts
    # 1. running user commands specified in Task "install_script" argument, successful completion writes to _install_script_fn
    # 2. executing launcher logic such as mounting EFS, setting up security keys, successful completion writes to _is_initialized_fn
    #
    # New task creation will check for those two files and skip corresponding setup tasks if present
    # Global variable NCLUSTER_FORCE_SETUP will force both parts to rerunset
    # task level kwarg skip_setup will allow skipping parts of setup regardless of previous setup runs succeeding
    # TODO(y): get rid of skip_setup kwarg, it's a rare case that can be set through env var

    self._initialized_fn = f'_ncluster_initialized'         # user initialization
    self._install_script_fn = f'_ncluster_install_script'   # launcher initialiation

    # Part 1: user initialization, running install script
    if not ncluster_globals.should_skip_setup():
      if not self._is_install_script_fn_present() and self.install_script:
        self.log("running install script")

        # bin/bash needed to make self-executable or use with UserData
        self.install_script = '#!/bin/bash\n' + self.install_script
        self.install_script += f'\necho ok > {self._install_script_fn}\n'
        self.file_write('install.sh', util.shell_add_echo(self.install_script))
        self.run('bash -e install.sh')  # fail on errors
        assert self._is_install_script_fn_present(), f"Install script didn't write to {self._install_script_fn}"

    # Part 2: launcher initialization
    if ncluster_globals.should_skip_setup():  # API-level flag ..make_task(..., skip_setup=True)
      should_skip_setup = True
    else:
      should_skip_setup = False               # default settings + new machine

    if not should_skip_setup:
      ##########################################
      # SSH setup
      ##########################################
      # 1) wait for keypair to be created (done by chief task)
      # 2) append public key to NCLUSTER_AUTHORIZED_KEYS
      # 3) propagate NCLUSTER_AUTHORIZED_KEYS to target machine and update ~/.ssh/authorized_keys

      # 1. wait for keypair creation, unless running under circleci env where keypairs are broken
      if not util.is_set('NCLUSTER_RUNNING_UNDER_CIRCLECI'):
        success = util.wait_for_file(util.ID_RSA_PUB)  # wait for chief task to create keypair
        assert success, f"Couldn't find {util.ID_RSA_PUB}"

      # 2. concat local public key with extra keys in NCLUSTER_AUTHORIZED_KEYS
      auth_keys_env_var_str = util.get_authorized_keys()

      # 3. dedup NCLUSTER_AUTHORIZED_KEYS and add to task environment + its ~/.ssh/authorized_keys
      auth_keys_file_str = '\n'
      seen_keys = set()
      for key in auth_keys_env_var_str.split(';'):
        if not key or key in seen_keys:
          continue
        seen_keys.add(key)
        assert key.count('ssh-rsa') <= 1, f"Malformed key, multiple ssh-rsa strings: '{key}'"
        if 'ssh-rsa' not in key:
          print(f"Warning, didn't see ssh-rsa in key {key}")
        auth_keys_file_str += key + '\n'
      self.run(f"""echo "{auth_keys_file_str}" >> ~/.ssh/authorized_keys""")

      # Mount file-systems
      self._mount_efs()
      # self._mount_tmpfs()

    self.propagate_env(['NCLUSTER_AUTHORIZED_KEYS',  # public keys that will work for passwordless SSH to machine
                        'WANDB_API_KEY'    # optional logging, if defined locally also propagate to remote machine
                        ])
    # TODO(y): this setting is only needed for streaming, maybe only enable it in that case
    self.run(f'export PYTHONUNBUFFERED=1')  # output tee piping turns on buffering in Python, turn it off

    self.connect_instructions = f"""To connect to {self.name} do "ncluster connect {self.name}" or
    ssh {self.ssh_username}@{self.public_ip}
    tmux a
    """.strip()

    self.write(self._initialized_fn, 'ok')
    self.log("Initialize complete")
    self.log(self.connect_instructions)

  def _is_initialized_fn_present(self):
    present = False
    try:
      present = 'ok' in self.read(self._initialized_fn)
    except Exception:
      pass
    self.log(f"Checking for initialized status: {present}")
    return present

  def _is_install_script_fn_present(self):
    present = False
    try:
      present = 'ok' in self.read(self._install_script_fn)
    except Exception:
      pass
    self.log(f"Checking for install_script status: {present}")
    return present

  def _setup_tmux(self):
    self.tmux_session = self.name.replace('.', '=')
    self.tmux_window_id = 0
    self.tmux_available_window_ids = [0]

    if not ncluster_globals.should_skip_setup():
      self.log("Setting up tmux")

      tmux_cmd = [f'tmux set-option -g history-limit 50000 \\; ',
                  f'set-option -g mouse on \\; ',
                  #                f'bind-key -n C-d detach \\; ',   # CTRL+d detaches instead of killing shell
                  f'new-session -s {self.tmux_session} -n 0 -d']

      # hack to get around Amazon linux not having tmux
      if self._linux_type == 'amazon':
        self.log("Amazon linux detected, installing tmux")
        self._run_raw('sudo yum install tmux -y')
        del tmux_cmd[1]  # Amazon tmux is really old, no mouse option

      self.log(f"Killing previous tmux session {self.tmux_session}")
      self._run_raw(f'tmux kill-session -t {self.tmux_session}', ignore_errors=True)
      self._run_raw(''.join(tmux_cmd))
    else:
      print("Warning, skip_setup is set, this will hang if remote tmux prompt is not available")

    self._can_run = True

  def _is_efs_mounted(self):
    stdout, stderr = self.run_with_output('df')
    return '/ncluster' in stdout

  def _mount_efs(self):
    if self._is_efs_mounted():
      self.log("Detected EFS, skipping remount")
      return

    self.log("Mounting ncluster EFS")
    region = u.get_region()
    efs_id = u.get_efs_dict()[u.get_prefix()]
    dns = f"{efs_id}.efs.{region}.amazonaws.com"
    self.run('sudo mkdir -p /ncluster && sudo chown `whoami` /ncluster')

    stdout, stderr = self.run_with_output('df')
    if '/ncluster' not in stdout:
      self.run(f"sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dns}:/ /ncluster",
               ignore_errors=True)

    # sometimes mount command doesn't work, make sure it's really mounted before returning
    stdout, stderr = self.run_with_output('df')
    while '/ncluster' not in stdout:
      sleep_sec = 2
      util.log(f"EFS not yet mounted, sleeping {sleep_sec} seconds")
      time.sleep(sleep_sec)
      self.run(
        f"sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dns}:/ /ncluster",
        ignore_errors=True)
      stdout, stderr = self.run_with_output('df')

    self.run('sudo chmod 777 /ncluster')

    # Hack below may no longer be needed
    # # make sure chmod is successful, hack to fix occasional permission errors
    # while 'drwxrwxrwx' not in self.run_and_capture_output('ls -ld /ncluster'):
    #   print(f"chmod 777 /ncluster didn't take, retrying in {TIMEOUT_SEC}")
    #   time.sleep(TIMEOUT_SEC)
    #   self.run('sudo chmod 777 /ncluster')

    # TODO(y): build a pstree and warn if trying to run something while main tmux bash has a subprocess running
    # this would ensure that commands being sent are not being swallowed

  def _mount_tmpfs(self):
    #  mount tmpfs if instance has enough memory
    # Mem:      503609216     1354612   478348012        9132    23906592   500624992
    util.log(f"Mounting tmpfs")

    line = ''   # silence the linters
    stdout, stderr = self.run_with_output('free -t -m')
    for line in stdout.split('\n'):
      if line.startswith('Mem'):
        break

    try:
      free_mb = int(line.split()[3])
    except Exception:
      assert False, "can't parse output of free: {stdout}"

    util.log(f"Instance has {free_mb} MB of free memory")
    if free_mb > 200:
      self.run("sudo mkdir -p /tmpfs && sudo chown `whoami` /tmpfs && sudo mount -t tmpfs -o size=100m tmpfs /tmpfs")
      util.log('tmpfs mounted at /tmpfs')
    else:
      util.log(f"Instance has only {free_mb} MB of memory, skipping tmpfs mount")

  def log(self, message, *args):
    """Log to launcher console."""
    if args:
      message %= args

    print(f"{util.current_timestamp()} {self.name}: {message}")

  # TODO: reuse regular run
  def run_with_output(self, cmd, non_blocking=False, ignore_errors=False) -> \
          Tuple[str, str]:
    """

    Args:
      cmd: single line shell command to run
      non_blocking (bool): if True, does not wait for command to finish
      ignore_errors: if True, will succeed even if command failed

    Returns:
      Contents of stdout/stderr as strings.
    Raises
      RuntimeException: if command produced non-0 returncode

    """

    assert '\n' not in cmd, "Do not support multi-line commands"
    cmd: str = cmd.strip()
    if not cmd or cmd.startswith('#'):  # ignore empty/commented out lines
      return '', ''

    stdout_fn = f"{self.remote_scratch}/{self.run_counter+1}.stdout"
    stderr_fn = f"{self.remote_scratch}/{self.run_counter+1}.stderr"
    cmd2 = f"{cmd} > {stdout_fn} 2> {stderr_fn}"

    assert not non_blocking, "Getting output doesn't work with non_blocking"
    status = self.run(cmd2, False, ignore_errors=True)
    stdout = self.read(stdout_fn)
    stderr = self.read(stderr_fn)

    if self.last_status > 0:
      self.log(f"Warning: command '{cmd}' returned {status},"
               f" stdout was '{stdout}' stderr was '{stderr}'")
      if not ignore_errors:
        raise RuntimeError(f"Warning: command '{cmd}' returned {status},"
                           f" stdout was '{stdout}' stderr was '{stderr}'")

    return stdout, stderr

  # TODO(y): make this kwarg only
  # TODO(y): document
  def run(self, cmd: Union[str, List[str]], sudo=False,
          non_blocking=False,
          ignore_errors=False,
          max_wait_sec=365 * 24 * 3600,
          check_interval=0.2,
          stream_output=False,
          sanitized=False,
          return_output=False):

    if isinstance(cmd, list):
      cmd = ' && '.join(['{ '+command+' ; }' for command in cmd])
    if sudo:
      cmd = f"sudo bash -c '{cmd}'"

    if not self._can_run:
      assert False, "Using .run before initialization finished"

    if '\n' in cmd:
      assert "'" in cmd or '"' in cmd, f"Your command '{cmd}' has newline but no quotes, are you sure?"

    cmd = cmd.strip()
    if cmd.startswith('#'):  # ignore empty/commented out lines
      return ''

    cmd_sanitized = cmd[:20]+'****' if sanitized else cmd

    self.run_counter += 1
    self.log("tmux> %s", cmd_sanitized)

    self._cmd = cmd
    # self._cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
    self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
    self._out_fn = f'{self.remote_scratch}/{self.run_counter}.out'

    cmd = util.shell_strip_comment(cmd)

    # wrap in { } so that 'cmd1||cmd2 > ...' works
    # https://www.gnu.org/software/bash/manual/html_node/Command-Grouping.html
    cmd = '{ '+cmd+'; }'

    # todo(y): is this check still needed?
    assert ' & ' not in cmd and not cmd.endswith('&'), f"cmd {cmd} contains &, that breaks things"

    # This takes half a second and only useful for debugging
    # self.file_write(self._cmd_fn, cmd + '\n')

    # modify command to dump shell success status into file
    # https://stackoverflow.com/a/692407/419116
    modified_cmd = f'{cmd} > >(tee -a {self._out_fn}) 2> >(tee -a {self._out_fn} >&2); echo $? > {self._status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    start_time = time.time()
    tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
    tmux_cmd = f"tmux send-keys -t {tmux_window} {modified_cmd} Enter"
    self._run_raw(tmux_cmd, ignore_errors=ignore_errors)

    tailer = util.NoOp()
    if stream_output:
      tailer = self.tail_file(self._out_fn, line_prefix='rr> ')

    if non_blocking:
      return 'nonblocking'

    elapsed_time = time.time() - start_time
    while not self.exists(self._status_fn) and elapsed_time < max_wait_sec:
      self.log(f"Still waiting for {cmd_sanitized}")
      self.wait_for_file(self._status_fn, max_wait_sec=60)
      elapsed_time = time.time() - start_time

    tailer.close()

    contents = self.read(self._status_fn)  # 0.5 sec

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(check_interval)
      contents = self.read(self._status_fn)
    status = int(contents.strip())
    self.last_status = status

    output = ''
    if return_output:
      output = self.output

    if status != 0:
      extra_msg = '(ignoring error)' if ignore_errors else '(failing)'
      if not output:
        output = self.output
      self.log(
        f"Start failing output {extra_msg}: \n{'*'*80}\n\n '{output}'")
      self.log(f"\n{'*'*80}\nEnd failing output")
      if not ignore_errors:
        raise RuntimeError(f"Command {cmd} returned status {status}")
      else:
        self.log(f"Warning: command {cmd} returned status {status}")

    if return_output:
      return output

  def propagate_env(self, env_vars: List[str], sanitized=True):
    """Propagates values of env_vars from client environment to the worker machine. IE
    task.propagate_env([AWS_SECRET_KEY, WANDB_API_KEY]) will set those vars on client machine to match the launching machine
    """
    for var in env_vars:
      if var in os.environ:
        # don't mirror env vars to stdout by default since they can contain secrets
        self.run(f'export {var}={shlex.quote(os.environ[var])}', sanitized=sanitized)

  def join(self, ignore_errors=False):
    """Waits until last executed command completed."""
    assert self._status_fn, "Asked to join a task which hasn't had any commands executed on it"
    check_interval = 0.2
    status_fn = self._status_fn
    if not self.wait_for_file(status_fn, max_wait_sec=30):
      self.log(f"Retrying waiting for {status_fn}")
    while not self.exists(status_fn):
      self.log(f"Still waiting for {self._cmd}")
      self.wait_for_file(status_fn, max_wait_sec=30)
    contents = self.read(status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(check_interval)
      contents = self.read(status_fn)
    status = int(contents.strip())
    self.last_status = status

    if status != 0:
      extra_msg = '(ignoring error)' if ignore_errors else '(failing)'
      self.log(f"Start failing output {extra_msg}: \n{'*'*80}\n\n '{self.output}'")
      self.log(f"\n{'*'*80}\nEnd failing output")
      if not ignore_errors:
        raise RuntimeError(f"Command {self._cmd} returned status {status}")
      else:
        self.log(f"Warning: command {self._cmd} returned status {status}")

    return status

  def _run_with_output_on_failure(self, cmd, non_blocking=False,
                                  ignore_errors=False,
                                  max_wait_sec=365 * 24 * 3600,
                                  check_interval=0.2,
                                  sanitized=False) -> str:
    """Experimental version of run propagates error messages to client. This command will be default "run" eventually"""

    pass

  def _run_raw(self, cmd: str, ignore_errors=False, sanitized=False) -> Tuple[str, str]:
    """Runs given cmd in the task using current SSH session, returns
    stdout/stderr as strings. Because it blocks until cmd is done, use it for
    short cmds.

    This is a barebones method to be used during initialization that have
    minimal dependencies (no tmux)
    """

    stdin, stdout, stderr = u.call_with_retries(self.ssh_client.exec_command,
                                                command=cmd, get_pty=True)
    stdout_str = stdout.read().decode()
    stderr_str = stderr.read().decode()
    cmd_sanitized = cmd[:20]+'****' if sanitized else cmd

    if stdout.channel.recv_exit_status() != 0:
      if not ignore_errors:
        self.log(f"command ({cmd_sanitized}) failed with --->")
        self.log("failing stdout: " + stdout_str)
        self.log("failing stderr: " + stderr_str)
        assert False, "_run_raw failed (see logs for error)"

    return stdout_str, stderr_str

  def rsync(self, local_fn: str, remote_fn: str = '', exclude_git=False):
    """Rsync dir to remote instance. If location not specified, dumps it into default directory."""
    if not remote_fn:
      remote_fn = os.path.basename(local_fn)
    remote_fn = remote_fn.replace('~', self.homedir)
    username = self.ssh_username
    hostname = self.public_ip
    excludes = " "
    excludes += "--exclude=\'.ssh/\' "
    excludes += "--exclude=\'.cache/\' "
    excludes += "--exclude=\'__pycache__/\' "
    excludes += "--exclude=\'wandb/run-\' "
    excludes += "--exclude=\'.eggs\' "

    if exclude_git:
      excludes += f"--exclude=\'.git/\' "
    cmd = (f'rsync -av {excludes} -e "ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no" ' +
           f'{local_fn} {username}@{hostname}:{remote_fn}')
    self.log(cmd)

    os.system(cmd)

  def upload(self, local_fn: str, remote_fn: str = '',
             dont_overwrite: bool = False) -> None:
    """Uploads file to remote instance. If location not specified, dumps it
    into default directory. If remote location has files or directories with the
     same name, behavior is undefined."""

    # support wildcard through glob
    if '*' in local_fn:
      for local_subfn in glob.glob(local_fn):
        self.upload(local_subfn)
      return

    if '#' in local_fn:  # hashes also give problems from shell commands
      self.log("skipping backup file {local_fn}")
      return

    if not self.sftp:
      self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                      'self.ssh_client.open_sftp')

    def maybe_fix_mode(local_fn_, remote_fn_):
      """Makes remote file execute for locally executable files"""
      mode = oct(os.stat(local_fn_)[stat.ST_MODE])[-3:]
      if '7' in mode:
        self.log(f"Making {remote_fn_} executable with mode {mode}")
        # use raw run, in case tmux is unavailable
        self._run_raw(f"chmod {mode} {remote_fn_}")

    # augmented SFTP client that can transfer directories, from
    # https://stackoverflow.com/a/19974994/419116
    def _put_dir(source, target):
      """ Uploads the contents of the source directory to the target path."""

      def _safe_mkdir(path, mode=511, ignore_existing=True):
        """ Augments mkdir by adding an option to not fail if the folder exists  asdf asdf asdf as"""
        try:
          self.sftp.mkdir(path, mode)
        except IOError:
          if ignore_existing:
            pass
          else:
            raise

      assert os.path.isdir(source)
      _safe_mkdir(target)

      for item in os.listdir(source):
        if os.path.isfile(os.path.join(source, item)):
          self.sftp.put(os.path.join(source, item), os.path.join(target, item))
          maybe_fix_mode(os.path.join(source, item), os.path.join(target, item))
        else:
          _safe_mkdir(f'{target}/{item}')
          _put_dir(f'{source}/{item}', f'{target}/{item}')

    if not remote_fn:
      remote_fn = os.path.basename(local_fn)

    # self.log('uploading ' + local_fn + ' to ' + remote_fn)
    remote_fn = remote_fn.replace('~', self.homedir)

    if '/' in remote_fn:
      remote_dir = os.path.dirname(remote_fn)
      assert self.exists(
        remote_dir), f"Remote dir {remote_dir} doesn't exist"
    if dont_overwrite and self.exists(remote_fn):
      self.log("Remote file %s exists, skipping" % (remote_fn,))
      return

    assert os.path.exists(local_fn), f"{local_fn} not found"
    if os.path.isdir(local_fn):
      _put_dir(local_fn, remote_fn)
    else:
      assert os.path.isfile(local_fn), "%s is not a file" % (local_fn,)
      # this crashes with IOError when upload failed
      if self.exists(remote_fn) and self.isdir(remote_fn):
        remote_fn = remote_fn + '/' + os.path.basename(local_fn)
      self.sftp.put(localpath=local_fn, remotepath=remote_fn)
      maybe_fix_mode(local_fn, remote_fn)

  def download(self, remote_fn, local_fn=''):
    if not self.sftp:
      self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                      'self.ssh_client.open_sftp')
    if not local_fn:
      local_fn = os.path.basename(remote_fn)
    remote_fn = remote_fn.replace('~', self.homedir)
    self.sftp.get(remote_fn, local_fn)

  def exists(self, remote_fn):
    if not self.sftp:
      self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                      'self.ssh_client.open_sftp')
    try:
      self.sftp.stat(remote_fn)
      return True
    except IOError:
      return False
    #    stdout, stderr = self._run_raw('stat ' + remote_fn, ignore_errors=True)
    #    return 'No such file' not in stdout

  def wait_for_file(self, fn: str, max_wait_sec: int = 3600 * 24 * 365,
                    check_interval: float = 0.02) -> bool:
    """
    Waits for file maximum of max_wait_sec. Returns True if file was detected within specified max_wait_sec
    Args:
      fn: filename on task machine
      max_wait_sec: how long to wait in seconds
      check_interval: how often to check in seconds
    Returns:
      False if waiting was was cut short by max_wait_sec limit, True otherwise
    """
    #    print("Waiting for file", fn)
    start_time = time.time()
    while True:
      if time.time() - start_time > max_wait_sec:
        util.log(f"Timeout exceeded ({max_wait_sec} sec) for {fn}")
        return False
      if not self.exists(fn):
        time.sleep(check_interval)
        continue
      else:
        break
    return True

  def write(self, remote_fn, contents):
    tmp_fn = self.local_scratch + '/' + str(util.now_micros())
    open(tmp_fn, 'w').write(contents)
    self.upload(tmp_fn, remote_fn)

  def read(self, remote_fn):
    tmp_fn = self.local_scratch + '/' + str(util.now_micros())
    self.download(remote_fn, tmp_fn)
    return open(tmp_fn).read()

  def isdir(self, remote_fn):
    stdout, _stderr = self._run_raw('ls -ld ' + remote_fn)
    return stdout.startswith('d')

  def switch_window(self, window_id: int):
    """
    Switches currently active tmux window for given task. 0 is the default window
    Args:
      window_id: integer id of tmux window to use
    """

    # windows are numbered sequentially 0, 1, 2, ...
    # create any missing windows and make them point to the same directory
    if window_id not in self.tmux_available_window_ids:
      for i in range(max(self.tmux_available_window_ids) + 1, window_id + 1):
        self._run_raw(f'tmux new-window -t {self.tmux_session} -d')
        self.tmux_available_window_ids.append(i)

    self.tmux_window_id = window_id

  @property
  def zone(self):
    """Returns zone of the instance associated with task or None if not available."""
    if self.instance:
      return self.instance.placement.get('AvailabilityZone', None)

  @property
  def num_gpus(self):
    return INSTANCE_INFO[self.instance.instance_type].get('gpus', 0)

  @property
  def output(self):
    """Returns console output of last command on this task."""
    return self.read(self._out_fn)

  @property
  def logdir(self):
    """Returns logging directory, creating one if necessary. See "Logdir" section
    of design doc on naming convention"""

    run_name = ncluster_globals.get_run_for_task(self)
    logdir = ncluster_globals.get_logdir(run_name)
    if logdir:
      return logdir

    # create logdir. Only single task in a group creates the logdir
    if ncluster_globals.is_chief(self, run_name):
      chief = self
    else:
      chief = ncluster_globals.get_chief(run_name)

    chief.setup_logdir()
    return ncluster_globals.get_logdir(run_name)

    # release lock

  def setup_logdir(self):
    # todo: locking on logdir creation

    """Create logdir for task/job/run
    """
    run_name = ncluster_globals.get_run_for_task(self)
    self.log("Creating logdir for run " + run_name)

    logdir_root = LOGDIR_ROOT

    # TODO(y): below can be removed, since we are mkdir -p later
    if not ncluster_globals.should_skip_setup():
      self._run_raw(f'mkdir -p {logdir_root}')
    find_command = f'find {logdir_root} -maxdepth 1 -type d'

    stdout, stderr = self.run_with_output(find_command)
    logdir = f"{logdir_root}/{run_name}"

    counter = 0
    while logdir in stdout:
      counter += 1
      new_logdir = f'{logdir_root}/{run_name}.{counter:02d}'
      logdir = new_logdir
    self._run_raw(f'mkdir -p {logdir}')

    ncluster_globals.set_logdir(run_name, logdir)
    return logdir

    # legacy methods
  def file_exists(self, remote_fn):
    return self.exists(remote_fn)

  def file_write(self, *args, **kwargs):
    return self.write(*args, **kwargs)

  def file_read(self, remote_fn):
    return self.read(remote_fn)

  def tail_file(self, fn: str, line_prefix='') -> "RemoteTailer":
    """Streams fn on task machine to client's console."""

    return RemoteTailer(self, fn, line_prefix)


class RemoteTailer:
  """Helper object to stream remote file to local machine asynchronously."""

  task: Task
  channel: paramiko.Channel
  t: threading.Thread

  def __init__(self, task: Task, fn: str, line_prefix: str = ''):
    self.task = task
    self.task._run_raw('mkdir -p ' + os.path.dirname(fn))
    self.task._run_raw('touch ' + fn)

    # _stdin, stdout, _stderr = self.ssh_client.exec_command(f'tail -f {fn}')
    _stdin, stdout, _stderr, channel = util.exec_command(task.ssh_client, f'tail -f {fn}')
    self.channel = channel

    def stream_func():
      for line in iter(lambda: stdout.readline(2048), b''):
        print(line_prefix, line.strip())
        sys.stdout.flush()
        if not line:   # EOF
          break

    self.t = threading.Thread(target=stream_func, name=f'RemoteTailer({fn})')
    self.t.start()

  def close(self):
    self.channel.close()
    self.task._run_raw('killall tail')   # hack https://github.com/paramiko/paramiko/issues/1475
    self.t.join(timeout=10)


class Job:
  """Collection of tasks close together."""

  name: str
  tasks: List[Task]

  #  run_: Run

  def __init__(self, name: str, tasks: List[Task] = None, **kwargs):
    """Initializes Job object, links tasks to refer back to the Job."""
    if tasks is None:
      tasks = []
    self.name = name
    self.tasks = tasks
    self.kwargs = kwargs
    # TODO: maybe backlinking is not needed
    for task in tasks:
      task.job = self

  @property
  def logdir(self):
    return self.tasks[0].logdir

  def _task_parallel(self, method, *args, **kwargs):
    """Runs given method on every task in the job in parallel. Blocks until all tasks finish. Propagates exception from first
    failed task."""

    exceptions = []

    def task_run(task):
      try:
        getattr(task, method)(*args, **kwargs)
      except Exception as e:
        exceptions.append(e)

    threads = [threading.Thread(name=f'task_{method}_{i}',
                                target=task_run, args=[t])
               for i, t in enumerate(self.tasks)]
    for thread in threads:
      thread.start()
    for thread in threads:
      while thread.is_alive():
        thread.join(timeout=30)
        if thread.is_alive():
          util.log(f"Job {self.name} still waiting for {thread.getName()}")

    if exceptions:
      raise exceptions[0]

  def run(self, *args, **kwargs):
    """Runs command on every task in the job in parallel, blocks until all tasks finish.
    See Task for documentation of args/kwargs."""
    return self._task_parallel("run", *args, **kwargs)

  def propagate_env(self, *args, **kwargs):
    """See py:func:`aws_backend.Task.propagate_env`"""
    return self._task_parallel("propagate_env", *args, **kwargs)

  def run_with_output(self, *args, **kwargs):
    """Runs command on every task in the job in parallel, blocks until all tasks finish.
    See Task for documentation of args/kwargs."""
    return self._task_parallel("run_with_output", *args, **kwargs)

  def rsync(self, *args, **kwargs):
    """See :py:func:`aws_backend.Task.rsync`"""
    return self._task_parallel("rsync", *args, **kwargs)

  def upload(self, *args, **kwargs):
    """See :py:func:`aws_backend.Task.upload`"""
    return self._task_parallel("upload", *args, **kwargs)

  def write(self, *args, **kwargs):
    return self._task_parallel("write", *args, **kwargs)

  def _run_raw(self, *args, **kwargs):
    return self._task_parallel("_run_raw", *args, **kwargs)


class Run:
  """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  """

  # TODO(y): get rid of this
  placement_group: str  # unique identifier to use as placement_group group name
  jobs: List[Job]

  def __init__(self, name='', **kwargs):
    """Creates a run. If install_script is specified, it's used as default
    install_script for all jobs (can be overridden by Job constructor)"""

    assert name, "Must specify name for current run"

    jobs = []
    self.name = name
    self.jobs = jobs
    self.kwargs = kwargs
    self.placement_group = name + '-' + util.random_id()

    if 'NCLUSTER_AWS_PLACEMENT_GROUP' in os.environ:
      placement_group = os.environ['NCLUSTER_AWS_PLACEMENT_GROUP']
      util.log(f"Overriding placement group through NCLUSTER_AWS_PLACEMENT_GROUP={placement_group}")
      self.placement_group = placement_group

    util.log(f"Choosing unique new placement_group for run {name} to be {self.placement_group}")

  @property
  def logdir(self):
    # querying logdir has a side-effect of creation, so do it on chief task
    chief_task = ncluster_globals.get_chief(self.name)
    return chief_task.logdir


def make_task(
        name: str = '',
        run_name: str = '',
        install_script: str = '',
        instance_type: str = '',
        image_name: str = '',
        disk_size: int = 500,
        create_resources=True,
        spot=False,
        is_chief=True,
        **_kwargs
) -> Task:
  """
  Create task on AWS.

  Automatically places it in singleton Run/singleton Job objects, see Run/Job/Task hierarchy for details
  https://docs.google.com/document/d/1Gg4T243cYrDUW1YDCikmqp7fzSQDU3rZxOkJr9ohhs8/edit#heading=h.j4td4oixogib


  Args:
    spot: try to reserve spot/preemptible instance
    disk_size: default size of root disk, in GBs
    create_resources: whether this task will handle resource creation
    name: see ncluster.make_task
    run_name: see ncluster.make_task
    install_script: see ncluster.make_task
    instance_type: instance type to use, defaults to $NCLUSTER_INSTANCE or t3.micro if unset
    image_name: name of image, ie, "Deep Learning AMI (Ubuntu) Version 12.0", defaults to generic small AWS image if unset
    logging_task: partially initialized Task object, use it for logging
    is_chief: if True, this is the task is charge of taking care of common configuration that only needs to be done once per run, such generating security key

  Returns:

  """

  ncluster_globals.task_launched = True

  logging_task = DummyTask(name=name)

  def log(*_args):
    logging_task.log(*_args)

  if is_chief:
    util.setup_local_ssh_keys()
    u.validate_local_keypair()

  # if name not specified, use name which is the same across script invocations for given image/instance-type
  name = ncluster_globals.auto_assign_task_name_if_needed(name, instance_type,
                                                          image_name)

  u.validate_run_name(run_name)
  u.validate_task_name(name)

  if not instance_type:
    instance_type: str = os.environ.get('NCLUSTER_INSTANCE', GENERIC_SMALL_INSTANCE)
    log("Using instance " + instance_type)

  if not image_name:
    image_name: str = os.environ.get('NCLUSTER_IMAGE', GENERIC_SMALL_IMAGE)

  _set_aws_environment()
  if create_resources:
    _maybe_create_resources(logging_task=logging_task)
  else:
    pass

  run: Run = ncluster_globals.get_run_object(run_name)
  instance = u.lookup_instance_exact(name, instance_type, image_name)

  image = u.lookup_image(image_name)
  log(f"Using image '{image_name}' ({image.id})")
  keypair = u.get_keypair()
  security_group = u.get_security_group()
  ec2 = u.get_ec2_resource()

  if instance:
    instance_reuse = True
    log(f"Reusing {instance}")
    if instance.state['Name'] == 'stopped':
      current_zone = u.get_zone()
      instance_zone = instance.placement['AvailabilityZone']
      if current_zone and current_zone != instance_zone:
        assert False, f"Can not reuse existing instance {name}, it is in zone {instance_zone} but currently enforcing zone {current_zone}. " \
          f"Either unset your NCLUSTER_ZONE for zone-agnostic config, or kill instance {name} to relaunch in new zone"

      instance.start()
      while True:
        log(f"Waiting  for {instance}({instance.state['Name']}) to start.")
        instance.reload()
        if instance.state['Name'] == 'running':
          break
        time.sleep(10)

  # create the instance if not present
  else:
    instance_reuse = False
    disk_size_str = f"({disk_size} GB)" if disk_size else ''
    log(f"Allocating {instance_type}{disk_size_str} for task {name}")
    args = {'ImageId': image.id,
            'InstanceType': instance_type,
            'MinCount': 1,
            'MaxCount': 1,
            'SecurityGroupIds': [security_group.id],
            'KeyName': keypair.name}

    linux_user = getpass.getuser()
    aws_user = u.get_iam_username()

    args['TagSpecifications'] = [{
      'ResourceType': 'instance',
      'Tags': [{
        'Key': 'Name',
        'Value': name
        },
        {'Key': 'User',
         'Value': aws_user},
        {'Key': 'UserLinux',
         'Value': linux_user},
        {'Key': 'ncluster',
         'Value': 'ncluster'},
      ]
    }]

    placement_specs = {}
    # if we are launching into placement group and using EFA, must use zone-specific network setup
    if ncluster_globals.is_enforced_placement_group() and u.instance_supports_efa(instance_type):
      u.assert_zone_specific_config()

      subnet = u.get_subnet()
      # Because of "AWS Security groups cannot be specified along with network interfaces", have to delete
      # security group spec from top level args: https://github.com/saltstack/salt/issues/25569
      args.pop('SecurityGroupIds', None)

      args['NetworkInterfaces'] = [{'SubnetId': subnet.id,
                                    'DeviceIndex': 0,
                                    'AssociatePublicIpAddress': True,
                                    'DeleteOnTermination': True,
                                    'InterfaceType': 'efa',
                                    'Groups': [security_group.id]}]
    if u.get_zone():
      placement_specs['AvailabilityZone'] = u.get_zone()

    if run and run.placement_group:
      placement_specs['GroupName'] = run.placement_group

    args['Placement'] = placement_specs
    args['Monitoring'] = {'Enabled': True}

    if disk_size:
      assert disk_size > 0
      ebs = {
        'VolumeSize': disk_size,
        'VolumeType': 'gp2',
      }

      args['BlockDeviceMappings'] = [{
        'DeviceName': image.root_device_name,
        'Ebs': ebs
      }]

    # Use high throughput disk (0.065/iops-month = about $1/hour)
    if util.is_set('NCLUSTER_AWS_FAST_ROOTDISK'):
      ebs = {
        'VolumeSize': disk_size if disk_size else 500,
        'VolumeType': 'io1',
        'Iops': 11500
      }

      args['BlockDeviceMappings'] = [{
        'DeviceName': image.root_device_name,
        'Ebs': ebs
      }]

    instances = []
    try:
      if spot:
        instances = u.create_spot_instances(args)
      else:
        assert not ncluster_globals.should_skip_setup(), "Creating new instances, but skip_setup is used, this setting is only to be used for reruns"
        instances = ec2.create_instances(**args)
    except Exception as e:
      log(f"Instance creation for {name} failed with ({e})")
      log("Terminating")
      os.kill(os.getpid(), signal.SIGTERM)  # sys.exit() doesn't work inside thread

    assert instances, f"ec2.create_instances returned {instances}"
    log(f"Allocated {len(instances)} instances")
    instance = instances[0]

  task = Task(name, instance=instance,
              install_script=install_script,
              image_name=image_name,
              instance_reuse=instance_reuse)

  ncluster_globals.register_task(task, run_name)
  return task


def make_job(
        name: str = '',
        run_name: str = '',
        num_tasks: int = 1,
        install_script: str = '',
        instance_type: str = '',
        image_name: str = '',
        create_resources=True,
        skip_setup=False,
        is_chief=True,
        **kwargs) -> Job:
  """
  Args:
    skip_setup: True to skip setup
    create_resources: if True, will create resources if necessary
    name: see backend.make_task
    run_name: see backend.make_task
    num_tasks: number of tasks to launch
    install_script: see make_task
    instance_type: see make_task
    image_name: see make_task
    skip_setup: skips various setup calls like mounting EFS/setup, can use it when job has already been created
    is_chief: for multi-job runs, set this to true for the first job, see make_task(..., is_chief)
  Returns:

  """

  ncluster_globals.set_should_skip_setup(skip_setup)
  assert u.instance_supports_placement_groups(instance_type), f"jobs supported only on instances that enable placement groups, current instance '{instance_type}' doesn't"
  ncluster_globals.enforce_placement_group()

  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"
  assert name.count(
    '.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"

  # TODO(y): get rid of these dummy tasks
  # dummy tasks for logging
  tasks = [DummyTask(f"{i}.{name}") for i in range(num_tasks)]

  _set_aws_environment(tasks[0])
  if create_resources:
    _maybe_create_resources(tasks[0])

  name = ncluster_globals.auto_assign_job_name_if_needed(name)
  run_name = ncluster_globals.auto_assign_run_name_if_needed(run_name)
  _run: Run = ncluster_globals.create_run_if_needed(run_name, make_run)

  if ncluster_globals.is_enforced_placement_group():
    assert u.instance_supports_placement_groups(instance_type)

  # only create placement group for 1. new instances, 2. support for placement and 3. launched as part of a group
  placement_group = _run.placement_group
  util.log(f"Launching '{name}' into placement_group group {placement_group}")
  u.maybe_create_placement_group(_run.placement_group)

  job = Job(name=name, run_name=run_name, **kwargs)

  # TODO(y): automatically retrieve placement group from existing instances
  if not util.is_set('NCLUSTER_AWS_PLACEMENT_GROUP'):
    names = {u.get_name(i) for i in u.lookup_instances(name, limit_to_current_user=True, states=['running', 'stopped'])}
    expected_names = {f'{i}.{name}' for i in range(num_tasks)}
    intersection = names.intersection(expected_names)
    assert not intersection or intersection == expected_names, f"Starting a new job where some instances are already created (names={names}, expected_names={expected_names}), must set NCLUSTER_AWS_PLACEMENT_GROUP manually, or delete {intersection}"

  exceptions = []

  # make tasks in parallel
  def make_task_fn(i: int):
    try:
      tasks[i] = make_task(f"{i}.{name}", run_name=run_name,
                           install_script=install_script,
                           instance_type=instance_type, image_name=image_name,
                           logging_task=tasks[i],
                           create_resources=False,
                           is_chief=(is_chief and i == 0),
                           # handle resources in job already
                           **kwargs)
    except Exception as e:
      exceptions.append(e)

  util.log("Creating threads")
  threads = [threading.Thread(name=f'make_task({i}.{name})',
                              target=make_task_fn, args=[i])
             for i in range(num_tasks)]
  for thread in threads:
    thread.start()

  for thread in threads:
    while thread.is_alive():
      thread.join(timeout=30)
      if thread.is_alive():
        util.log(f"still waiting for thread {thread.getName()}")

  print("Exception are ", exceptions)
  if exceptions:
    raise exceptions[0]

  job.tasks = tasks

  # double check that all instances are in the same placement_group group
  # this can happen if some instances from previous smaller run are getting reused
  placement_dict = {task.instance.placement_group: task.name for task in
                    job.tasks}
  # TODO: make placement_group group name derived from run, to make it deterministic
  # on individual instance restarts
  if len(placement_dict) > 1:
    util.log("Job tasks are spread over multiple placement_group groups")
    pprint.pprint(placement_dict)
    raise RuntimeError(
      f"Got instance spread over multiple placement_group groups: {placement_dict}. Must terminate all instances in run {run_name} and try again.")
  ncluster_globals.unenforce_placement_group()

  return job


def make_run(name) -> Run:
  run = Run(name)
  ncluster_globals.register_run(run, name)
  return run


def _maybe_start_instance(instance: Instance) -> None:
  """Starts instance if it's stopped, no-op otherwise."""

  if instance.state['Name'] == 'stopped':
    instance.start()
    while True:
      print(f"Waiting  for {instance} to start.")
      instance.reload()
      if instance.state['Name'] == 'running':
        break
      time.sleep(10)


def _maybe_wait_for_initializing_instance(instance):
  """Starts instance if it's stopped, no-op otherwise."""

  if not instance:
    return

  if instance.state['Name'] == 'initializing':
    while True:
      print(f"Waiting  for {instance} to leave state 'initializing'.")
      instance.reload()
      if instance.state['Name'] == 'running':
        break
      time.sleep(10)


def _maybe_create_resources(logging_task: Task = None):
  """Use heuristics to decide to possibly create resources"""

  def log(*args):
    if logging_task:
      logging_task.log(*args)
    else:
      util.log(*args)

  def should_create_resources():
    """Check if gateway, keypair, vpc exist."""
    prefix = u.get_prefix()
    if u.get_keypair_name() not in u.get_keypair_dict():
      log(f"Missing {u.get_keypair_name()} keypair, creating resources")
      return True
    vpcs = u.get_vpc_dict()
    # TODO(y): this heuristic is now too generous, since we use default VPC for everything instead of ncluster-specific
    if prefix not in vpcs:
      log(f"Missing {prefix} vpc, creating resources")
      return True
    vpc = vpcs[prefix]
    gateways = u.get_gateway_dict(vpc)
    if prefix not in gateways:
      log(f"Missing {prefix} gateway, creating resources")
      return True
    return False

  if not should_create_resources() and not util.is_set('NCLUSTER_AWS_FORCE_CREATE_RESOURCES'):
    util.log("AWS resources probably already created, skipping. Use NCLUSTER_AWS_FORCE_CREATE_RESOURCES to force")
    return

  util.log(f"Acquiring AWS resource creation lock {AWS_LOCK_FN}")
  with portalocker.Lock(AWS_LOCK_FN, timeout=3600*24*365) as _fh:
    util.log(f"Success, AWS resource creation lock {AWS_LOCK_FN} acquired")
    create_lib.create_resources()

    # _fh.flush()
    # _os.fsync(fh.fileno())


def _set_aws_environment(task: Task = None):
  """Sets up AWS environment from NCLUSTER environment variables and/or boto3 session"""
  current_zone = os.environ.get('NCLUSTER_ZONE', '')
  current_region = os.environ.get('AWS_DEFAULT_REGION', '')

  def log(*args):
    if task:
      task.log(*args)
    else:
      util.log(*args)

  if current_region and current_zone:
    assert current_zone.startswith(
      current_region), f'Current zone "{current_zone}" ($NCLUSTER_ZONE) is not ' \
                       f'in current region "{current_region} ($AWS_DEFAULT_REGION)'
    assert u.get_session().region_name == current_region  # setting from ~/.aws

  # zone is set, set region from zone
  if current_zone:
    new_current_region = current_zone[:-1]
    if current_region and new_current_region != current_region:
      print(f"Warning, AWS_DEFAULT_REGION={current_region} conflicts with NCLUSTER_ZONE={current_zone}, changing AWS_DEFAULT_REGION to {new_current_region}")
    os.environ['AWS_DEFAULT_REGION'] = new_current_region

  # neither zone nor region not set, use default setting for region
  # if default is not set, use NCLUSTER_DEFAULT_REGION
  if not current_region:
    current_region = u.get_session().region_name
    if not current_region:
      log(f"No default region available, using {NCLUSTER_DEFAULT_REGION}")
      current_region = NCLUSTER_DEFAULT_REGION
    os.environ['AWS_DEFAULT_REGION'] = current_region

  # zone not set, use first zone of the region
  #  if not current_zone:
  #    current_zone = current_region + 'a'
  #    os.environ['NCLUSTER_ZONE'] = current_zone

  log(f"Using account {u.get_account_number()}:{u.get_account_name()}, region {current_region}, zone {current_zone}")
