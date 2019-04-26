"""AWS implementation of backend.py

Not thread-safe
"""
import glob
import os
import pprint
import shlex
import signal
import stat
import threading
import time
from typing import Tuple, List

import paramiko

from ncluster import ncluster_globals

from . import aws_create_resources as create_lib
from . import aws_util as u
from . import backend
from . import util

TMPDIR = '/tmp/ncluster'  # location for temp files on launching machine
AWS_LOCK_FN = '/tmp/aws.lock'  # lock file used to prevent concurrent creation of AWS resources by multiple workers in parallel
NCLUSTER_DEFAULT_REGION = 'us-east-1'  # used as last resort if no other method set a region

# default value of logdir root for this backend (can override with set_logdir_root)
DEFAULT_LOGDIR_ROOT = '/ncluster/runs'

# some image which is fast to load, to use for quick runs
GENERIC_SMALL_IMAGE = 'amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2'


class Task(backend.Task):
  """AWS task is initialized with an AWS instance and handles initialization,
  creation of SSH session, shutdown"""
  last_status: int  # status of last command executed

  tmux_window_id: int
  tmux_available_window_ids: List[int]

  sftp: paramiko.SFTPClient

  def __init__(self, name, *, instance, install_script='', image_name='',
               **extra_kwargs):
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
    self._cmd_fn = None
    self._cmd = None
    self._status_fn = None  # location of output of last status
    self.last_status = -1

    self._can_run = False  # indicates that things needed for .run were created
    self.initialize_called = False

    self.name = name
    self.instance = instance
    self.install_script = install_script
    self.extra_kwargs = extra_kwargs

    self.public_ip = u.get_public_ip(instance)
    self.ip = u.get_ip(instance)
    self.sftp = None
    self._linux_type = 'ubuntu'

    # heuristic to tell if I'm using Amazon image name
    # default image has name like 'amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2'
    if 'amzn' in image_name.lower() or 'amazon' in image_name.lower():
      self.log('Detected Amazon Linux image')
      self._linux_type = 'amazon'
    self.run_counter = 0

    launch_id = util.random_id()
    self.local_scratch = f"{TMPDIR}/{name}-{launch_id}"
    self.remote_scratch = f"{TMPDIR}/{name}-{launch_id}"

    os.system('mkdir -p ' + self.local_scratch)

    self._initialized_fn = f'is_initialized'

    # _current_directory tracks current directory on task machine
    # used for uploading without specifying absolute path on target machine
    if self._linux_type == 'ubuntu':
      #      self._current_directory = '/home/ubuntu'
      self.ssh_username = 'ubuntu'  # default username on task machine
    elif self._linux_type == 'amazon':
      #      self._current_directory = '/home/ec2-user'
      self.ssh_username = 'ec2-user'
    self.homedir = '/home/' + self.ssh_username

    self.ssh_client = u.ssh_to_task(self)
    self._setup_tmux()
    self._run_raw('mkdir -p ' + self.remote_scratch)

    self._can_run = True

    if self._is_initialized_fn_present():
      self.log("reusing previous initialized state")
    else:
      self.log("running install script")

      # bin/bash needed to make self-executable or use with UserData
      self.install_script = '#!/bin/bash\n' + self.install_script
      self.install_script += f'\necho ok > {self._initialized_fn}\n'
      self.file_write('install.sh', util.shell_add_echo(self.install_script))
      self.run('bash -e install.sh')  # fail on errors
      assert self._is_initialized_fn_present(), f"Install script didn't write to {self._initialized_fn}"

    self._mount_efs()
    self.connect_instructions = f"""
    To connect to {self.name}
ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no {self.ssh_username}@{self.public_ip}
tmux a
""".strip()
    self.log("Initialize complete")
    self.log(self.connect_instructions)

  def _is_initialized_fn_present(self):
    self.log("Checking for initialization status")
    try:
      return 'ok' in self.read(self._initialized_fn)
    except Exception:
      return False

  def _setup_tmux(self):
    self.log("Setting up tmux")

    self.tmux_session = self.name.replace('.', '=')
    self.tmux_window_id = 0
    self.tmux_available_window_ids = [0]

    tmux_cmd = [f'tmux set-option -g history-limit 50000 \; ',
                f'set-option -g mouse on \; ',
                f'new-session -s {self.tmux_session} -n 0 -d']

    # hack to get around Amazon linux not having tmux
    if self._linux_type == 'amazon':
      self._run_raw('sudo yum install tmux -y')
      del tmux_cmd[1]  # Amazon tmux is really old, no mouse option

    if not util.is_set("NCLUSTER_NOKILL_TMUX"):
      self._run_raw(f'tmux kill-session -t {self.tmux_session}',
                    ignore_errors=True)
    else:
      print(
        "Warning, NCLUSTER_NOKILL_TMUX is on, make sure remote tmux prompt is available or things will hang")

    self._run_raw(''.join(tmux_cmd))

    self._can_run = True

  def _mount_efs(self):
    self.log("Mounting EFS")
    region = u.get_region()
    efs_id = u.get_efs_dict()[u.get_prefix()]
    dns = f"{efs_id}.efs.{region}.amazonaws.com"
    self.run('sudo mkdir -p /ncluster')

    # ignore error on remount (efs already mounted)
    self.run(
      f"sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dns}:/ /ncluster",
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

  def run(self, cmd, non_blocking=False, ignore_errors=False,
          max_wait_sec=365 * 24 * 3600,
          check_interval=0.2):

    # TODO(y): make _run_with_output_on_failure default, and delete this
    if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE') or True:
      # experimental version that captures output and prints it on failure
      # redirection things break bash commands, so
      # don't redirect on bash commands like source
      # TODO(y): remove this, put in this filtering becase I thought it broke
      # source activate, but now it seems it doesn't
      if not util.is_bash_builtin(cmd) or True:
        return self._run_with_output_on_failure(cmd, non_blocking,
                                                ignore_errors,
                                                max_wait_sec)
      else:
        self.log("Found bash built-in, using regular run")

    if not self._can_run:
      assert False, "Using .run before initialization finished"

    if '\n' in cmd:
      cmds = cmd.split('\n')
      self.log(
        f"Running {len(cmds)} commands at once, returning status of last")
      status = -1
      for subcmd in cmds:
        status = self.run(subcmd)
        self.last_status = status
      return status

    cmd = cmd.strip()
    if cmd.startswith('#'):  # ignore empty/commented out lines
      return -1
    self.run_counter += 1
    self.log("tmux> %s", cmd)

    self._cmd = cmd
    self._cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
    self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'

    cmd = util.shell_strip_comment(cmd)
    assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    # modify command to dump shell success status into file
    self.file_write(self._cmd_fn, cmd + '\n')
    modified_cmd = f'{cmd}; echo $? > {self._status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
    tmux_cmd = f'tmux send-keys -t {tmux_window} {modified_cmd} Enter'
    self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
    if non_blocking:
      return 0

    if not self.wait_for_file(self._status_fn, max_wait_sec=30):
      self.log(f"Retrying waiting for {self._status_fn}")
    while not self.exists(self._status_fn):
      self.log(f"Still waiting for {cmd}")
      self.wait_for_file(self._status_fn, max_wait_sec=30)
    contents = self.read(self._status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(check_interval)
      contents = self.read(self._status_fn)
    status = int(contents.strip())
    self.last_status = status

    if status != 0:
      if not ignore_errors:
        raise RuntimeError(f"Command {cmd} returned status {status}")
      else:
        self.log(f"Warning: command {cmd} returned status {status}")

    return status

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
      if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE') or True:
        self.log(
          f"Start failing output {extra_msg}: \n{'*'*80}\n\n '{self.read(self._out_fn)}'")
        self.log(f"\n{'*'*80}\nEnd failing output")
      if not ignore_errors:
        raise RuntimeError(f"Command {self._cmd} returned status {status}")
      else:
        self.log(f"Warning: command {self._cmd} returned status {status}")

    return status

  def _run_with_output_on_failure(self, cmd, non_blocking=False,
                                  ignore_errors=False,
                                  max_wait_sec=365 * 24 * 3600,
                                  check_interval=0.2) -> str:
    """Experimental version of run propagates error messages to client. This command will be default "run" eventually"""

    if not self._can_run:
      assert False, "Using .run before initialization finished"

    if '\n' in cmd:
      assert False, "Don't support multi-line for run2"

    cmd = cmd.strip()
    if cmd.startswith('#'):  # ignore empty/commented out lines
      return ''
    self.run_counter += 1
    self.log("tmux> %s", cmd)

    self._cmd = cmd
    self._cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
    self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
    self._out_fn = f'{self.remote_scratch}/{self.run_counter}.out'

    cmd = util.shell_strip_comment(cmd)
    assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    # modify command to dump shell success status into file
    self.file_write(self._cmd_fn, cmd + '\n')

    #    modified_cmd = f'{cmd} > {out_fn} 2>&1; echo $? > {status_fn}'
    # https://stackoverflow.com/a/692407/419116
    # $cmd > >(tee -a fn) 2> >(tee -a fn >&2)

    modified_cmd = f'{cmd} > >(tee -a {self._out_fn}) 2> >(tee -a {self._out_fn} >&2); echo $? > {self._status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    start_time = time.time()
    tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
    tmux_cmd = f"tmux send-keys -t {tmux_window} {modified_cmd} Enter"
    self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
    if non_blocking:
      return 0

    if not self.wait_for_file(self._status_fn, max_wait_sec=60):
      self.log(f"Retrying waiting for {self._status_fn}")
    elapsed_time = time.time() - start_time
    while not self.exists(self._status_fn) and elapsed_time < max_wait_sec:
      self.log(f"Still waiting for {cmd}")
      self.wait_for_file(self._status_fn, max_wait_sec=60)
      elapsed_time = time.time() - start_time
    contents = self.read(self._status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(check_interval)
      contents = self.read(self._status_fn)
    status = int(contents.strip())
    self.last_status = status

    if status != 0:
      extra_msg = '(ignoring error)' if ignore_errors else '(failing)'
      self.log(
        f"Start failing output {extra_msg}: \n{'*'*80}\n\n '{self.read(self._out_fn)}'")
      self.log(f"\n{'*'*80}\nEnd failing output")
      if not ignore_errors:
        raise RuntimeError(f"Command {cmd} returned status {status}")
      else:
        self.log(f"Warning: command {cmd} returned status {status}")

    return self.read(self._out_fn)

  def _run_raw(self, cmd: str, ignore_errors=False) -> Tuple[str, str]:
    """Runs given cmd in the task using current SSH session, returns
    stdout/stderr as strings. Because it blocks until cmd is done, use it for
    short cmds. Silently ignores failing commands.

    This is a barebones method to be used during initialization that have
    minimal dependencies (no tmux)
    """
    #    self._log("run_ssh: %s"%(cmd,))

    stdin, stdout, stderr = u.call_with_retries(self.ssh_client.exec_command,
                                                command=cmd, get_pty=True)
    stdout_str = stdout.read().decode()
    stderr_str = stderr.read().decode()
    if stdout.channel.recv_exit_status() != 0:
      if not ignore_errors:
        self.log(f"command ({cmd}) failed with --->")
        self.log("failing stdout: " + stdout_str)
        self.log("failing stderr: " + stderr_str)
        assert False, "_run_raw failed (see logs for error)"

    return stdout_str, stderr_str

  def rsync(self, local_fn: str, remote_fn: str = ''):
    """Rsync dir to remote instance. If location not specified, dumps it into default directory."""
    if not remote_fn:
      remote_fn = os.path.basename(local_fn)
    remote_fn = remote_fn.replace('~', self.homedir)
    username = self.ssh_username
    hostname = self.public_ip
    cmd = (f'rsync -av -e "ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no" ' +
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

    self.log('uploading ' + local_fn + ' to ' + remote_fn)
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
    self.log("downloading %s" % remote_fn)
    # sometimes open_sftp fails with Administratively prohibited, do retries
    # root cause could be too many SSH connections being open
    # https://unix.stackexchange.com/questions/14160/ssh-tunneling-error-channel-1-open-failed-administratively-prohibited-open
    if not self.sftp:
      self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                      'self.ssh_client.open_sftp')
    if not local_fn:
      local_fn = os.path.basename(remote_fn)
      self.log("downloading %s to %s" % (remote_fn, local_fn))
    self.sftp.get(remote_fn, local_fn)

  def exists(self, remote_fn):
    stdout, stderr = self._run_raw('stat ' + remote_fn, ignore_errors=True)
    return 'No such file' not in stdout

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
    logdir_root = ncluster_globals.LOGDIR_ROOT
    assert logdir_root, "LOGDIR_ROOT not set, make sure you have called ncluster.set_backend()"

    self.run(f'mkdir -p {logdir_root}')
    find_command = f'find {logdir_root} -maxdepth 1 -type d'

    stdout, stderr = self.run_with_output(find_command)
    logdir = f"{logdir_root}/{run_name}"

    counter = 0
    while logdir in stdout:
      counter += 1
      new_logdir = f'{logdir_root}/{run_name}.{counter:02d}'
      self.log(f'Warning, logdir {logdir} exists, deduping to {new_logdir}')
      logdir = new_logdir
    self.run(f'mkdir -p {logdir}')

    ncluster_globals.set_logdir(run_name, logdir)
    return logdir

    # legacy methods
  def file_exists(self, remote_fn):
    return self.exists(remote_fn)

  def file_write(self, *args, **kwargs):
    return self.write(*args, **kwargs)

  def file_read(self, remote_fn):
    return self.read(remote_fn)

class Job(backend.Job):
  pass


class Run(backend.Run):
  """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  :ivar aws_placement_group_name: somedoc
  """
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
    util.log(f"Choosing placement_group for run {name} to be {self.placement_group}")

  @property
  def logdir(self):
    # querying logdir has a side-effect of creation, so do it on chief task
    chief_task = ncluster_globals.get_chief(self.name)
    return chief_task.logdir

  # TODO: currently this is synchronous, use non_blocking wrapper like in Job to parallelize methods
  def run(self, *args, **kwargs):
    """Runs command on every job in the run."""

    for job in self.jobs:
      job.run(*args, **kwargs)

  def run_with_output(self, *args, **kwargs):
    """Runs command on every first job in the run, returns stdout."""
    for job in self.jobs:
      job.run_with_output(*args, **kwargs)

  def _run_raw(self, *args, **kwargs):
    """_run_raw on every job in the run."""
    for job in self.jobs:
      job._run_raw(*args, **kwargs)

  def upload(self, *args, **kwargs):
    """Runs command on every job in the run."""
    for job in self.jobs:
      job.upload(*args, **kwargs)

  def make_job(self, name='', **kwargs):
    return make_job(name+'.'+self.name, run_name=self.name, **kwargs)


def make_task(
        name: str = '',
        run_name: str = '',
        install_script: str = '',
        instance_type: str = '',
        image_name: str = '',
        disk_size: int = 0,
        preemptible=None,
        logging_task: backend.Task = None,
        create_resources=True,
        spot=False
) -> Task:
  """
  Create task on AWS.

  Automatically places it in singleton Run/singleton Job objects, see Run/Job/Task hierarchy for details
  https://docs.google.com/document/d/1Gg4T243cYrDUW1YDCikmqp7fzSQDU3rZxOkJr9ohhs8/edit#heading=h.j4td4oixogib


  Args:
    disk_size: default size of root disk, in GBs
    create_resources: whether this task will handle resource creation
    name: see ncluster.make_task
    run_name: see ncluster.make_task
    install_script: see ncluster.make_task
    instance_type: instance type to use, defaults to $NCLUSTER_INSTANCE or t3.micro if unset
    image_name: name of image, ie, "Deep Learning AMI (Ubuntu) Version 12.0", defaults to $NCLUSTER_IMAGE or amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2 if unset
    preemptible: use cheaper preemptible/spot instances
    logging_task: partially initialized Task object, use it for logging

  Returns:

  """

  ncluster_globals.task_launched = True

  def log(*_args):
    if logging_task:
      logging_task.log(*_args)
    else:
      util.log(*_args)

  # if name not specified, use name which is the same across script invocations for given image/instance-type
  name = ncluster_globals.auto_assign_task_name_if_needed(name, instance_type,
                                                          image_name)

  if not instance_type:
    instance_type = os.environ.get('NCLUSTER_INSTANCE', 't3.micro')
    log("Using instance " + instance_type)

  _set_aws_environment()
  if create_resources:
    _maybe_create_resources(logging_task=logging_task)
  else:
    pass

  run: Run = ncluster_globals.get_run_object(run_name)
  placement_group = ''
  if ncluster_globals.is_enforced_placement_group():
    assert u.instance_supports_placement_groups(instance_type)
    assert run

  if u.instance_supports_placement_groups(instance_type) and run:
    placement_group = run.placement_group
    log(f"Launching into placement_group group {placement_group}")
    u.maybe_create_placement_group(run.placement_group)

  if not image_name:
    image_name = os.environ.get('NCLUSTER_IMAGE',
                                GENERIC_SMALL_IMAGE)
  log("Using image " + image_name)

  if preemptible is None:
    preemptible = os.environ.get('NCLUSTER_PREEMPTIBLE', False)
    preemptible = bool(preemptible)
    if preemptible:
      log("Using preemptible instances")

  image = u.lookup_image(image_name)
  keypair = u.get_keypair()
  security_group = u.get_security_group()
  ec2 = u.get_ec2_resource()

  instance = u.lookup_instance(name, instance_type,
                               image_name)
  _maybe_start_instance(instance)
  _maybe_wait_for_initializing_instance(instance)

  # create the instance if not present
  if instance:
    log(f"Reusing {instance}")
  else:
    log(f"Allocating {instance_type} for task {name}")
    args = {'ImageId': image.id,
            'InstanceType': instance_type,
            'MinCount': 1,
            'MaxCount': 1,
            'SecurityGroupIds': [security_group.id],
            'KeyName': keypair.name}

    args['TagSpecifications'] = [{
      'ResourceType': 'instance',
      'Tags': [{
        'Key': 'Name',
        'Value': name
      }]
    }]

    #    subnet = u.get_subnet()
    #    args['NetworkInterfaces'] = [{'SubnetId': subnet.id,
    #                                  'DeviceIndex': 0,
    #                                  'AssociatePublicIpAddress': True,
    #                                  'Groups': [security_group.id]}]
    #    placement_specs = {'AvailabilityZone': u.get_zone()}

    placement_specs = {}
    if placement_group:
      placement_specs['GroupName'] = placement_group

    args['Placement'] = placement_specs
    args['Monitoring'] = {'Enabled': True}

    if disk_size:
      assert disk_size > 0
      ebs = {
        'VolumeSize': disk_size,
        'VolumeType': 'gp2',
      }

      args['BlockDeviceMappings'] = [{
        'DeviceName': '/dev/sda1',
        'Ebs': ebs
      }]

    # Use high throughput disk (0.065/iops-month = about $1/hour)
    if 'NCLUSTER_AWS_FAST_ROOTDISK' in os.environ:
      assert not disk_size, f"Specified both disk_size {disk_size} and $NCLUSTER_AWS_FAST_ROOTDISK, they are incompatible as $NCLUSTER_AWS_FAST_ROOTDISK hardwired disk size"

      ebs = {
        'VolumeSize': 500,
        'VolumeType': 'io1',
        'Iops': 11500
      }

      args['BlockDeviceMappings'] = [{
        'DeviceName': '/dev/sda1',
        'Ebs': ebs
      }]

    instances = []
    try:
      if spot:
        instances = u.create_spot_instances(args)
      else:
        instances = ec2.create_instances(**args)
    except Exception as e:
      log(f"Instance creation for {name} failed with ({e})")
      log(
        "You can change availability zone using export NCLUSTER_ZONE=...")
      log("Terminating")
      os.kill(os.getpid(),
              signal.SIGINT)  # sys.exit() doesn't work inside thread

    assert instances, f"ec2.create_instances returned {instances}"
    log(f"Allocated {len(instances)} instances")
    instance = instances[0]

  task = Task(name, instance=instance,
              install_script=install_script,
              image_name=image_name,
              instance_type=instance_type)

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
        **kwargs) -> Job:
  """
  Args:
    create_resources: if True, will create resources if necessary
    name: see backend.make_task
    run_name: see backend.make_task
    num_tasks: number of tasks to launch
    install_script: see make_task
    instance_type: see make_task
    image_name: see make_task

  Returns:

  """

  assert u.instance_supports_placement_groups(instance_type), f"jobs supported only on instances that enable placement groups, current instance {instance_type} doesn't"
  ncluster_globals.enforce_placement_group()

  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"
  assert name.count(
    '.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"

  # dummy tasks for logging
  tasks = [backend.Task(f"{i}.{name}") for i in range(num_tasks)]

  _set_aws_environment(tasks[0])
  if create_resources:
    _maybe_create_resources(tasks[0])

  name = ncluster_globals.auto_assign_job_name_if_needed(name)
  run_name = ncluster_globals.auto_assign_run_name_if_needed(run_name)
  _run = ncluster_globals.create_run_if_needed(run_name, make_run)

  job = Job(name=name, tasks=tasks, run_name=run_name, **kwargs)

  exceptions = []

  # make tasks in parallel
  def make_task_fn(i: int):
    try:
      tasks[i] = make_task(f"{i}.{name}", run_name=run_name,
                           install_script=install_script,
                           instance_type=instance_type, image_name=image_name,
                           logging_task=tasks[i],
                           create_resources=False,
                           # handle resources in job already
                           **kwargs)
    except Exception as e:
      exceptions.append(e)

  util.log("Creating threads")
  threads = [threading.Thread(name=f'make_task_{i}',
                              target=make_task_fn, args=[i])
             for i in range(num_tasks)]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()
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
  return job


def make_run(name) -> Run:
  run = Run(name)
  ncluster_globals.register_run(run, name)
  return run


# TODO: this method and a few others are backend specific, document in API doc
def _maybe_start_instance(instance):
  """Starts instance if it's stopped, no-op otherwise."""

  if not instance:
    return

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
    if prefix not in vpcs:
      log(f"Missing {prefix} vpc, creating resources")
      return True
    vpc = vpcs[prefix]
    gateways = u.get_gateway_dict(vpc)
    if prefix not in gateways:
      log(f"Missing {prefix} gateway, creating resources")
      return True
    return False

  try:
    # this locking is approximate, still possible for threads to slip through
    if os.path.exists(AWS_LOCK_FN):
      pid, ts, lock_taskname = open(AWS_LOCK_FN).read().split('-')
      ts = int(ts)
      log(f"waiting for aws resource creation, another resource initiation was "
          f"initiated {int(time.time()-ts)} seconds ago by "
          f"{lock_taskname}, delete lock file "
          f"{AWS_LOCK_FN} if this is an error")
      while True:
        if os.path.exists(AWS_LOCK_FN):
          log(f"waiting for lock file {AWS_LOCK_FN} to get deleted "
              f"initiated {int(time.time()-ts)} seconds ago by ")
          time.sleep(2)
          continue
        else:
          break
      return

    with open(AWS_LOCK_FN, 'w') as f:
      f.write(
        f'{os.getpid()}-{int(time.time())}-{logging_task.name if logging_task else ""}')

    if not should_create_resources():
      util.log("Resources already created, no-op")
      os.remove(AWS_LOCK_FN)
      return

    create_lib.create_resources()
  finally:
    if os.path.exists(AWS_LOCK_FN):
      os.remove(AWS_LOCK_FN)


def _set_aws_environment(task: Task = None):
  """Sets up AWS environment from NCLUSTER environment variables"""
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
  if current_zone and not current_region:
    current_region = current_zone[:-1]
    os.environ['AWS_DEFAULT_REGION'] = current_region

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

  log(f"Using account {u.get_account_number()}, region {current_region}, "
      f"zone {current_zone}")

