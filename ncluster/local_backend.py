"""Local implementation of backend.py using separate tmux sessions for jobs.

Not thread-safe.
"""

import glob
import os
import shlex
import socket
import time
from typing import List

from ncluster import ncluster_globals
from . import backend
from . import util

TASKDIR_ROOT = '/tmp/ncluster/task'
SCRATCH_ROOT = '/tmp/ncluster/scratch'
#LOGDIR_ROOT = os.environ['HOME'] + '/ncluster/runs'  # use ~ instead of /tmp because /tmp gets wiped
DEFAULT_LOGDIR_ROOT = '/ncluster/runs'


# todo: tmux session names are backwards from AWS job names (runname-jobname)
# TODO: add kwargs so that tmux backend can be drop-in replacement


# TODO: rename extra_kwargs to kwargs everywhere
class Task(backend.Task):
  """Local tasks interact with tmux session where session name is derived
  from job name, and window names are task ids."""
  tmux_window_id: int
  tmux_available_window_ids: List[int]

  def __init__(self, name, *, tmux_session, install_script='', job=None,
               **kwargs):

    self.homedir = os.environ['HOME']
    self._cmd_fn = None
    self._cmd = None
    self._status_fn = None  # location of output of last status
    self._out_fn = None

    self._can_run = False
    self.tmux_session = tmux_session
    self.tmux_window_id = 0
    self.tmux_available_window_ids = [0]

    self.name = name
    self.install_script = install_script
    self.job = job
    self.kwargs = kwargs

    # local servers sometimes listen only on localhost (TensorBoard), and sometimes only on
    # externally assigned ip address from gethostbyname (Ray), must choose one, so use the localhost for TB compatibility
    # https://github.com/ray-project/ray/issues/1677
    self.public_ip = socket.gethostbyname(socket.gethostname())
    #  self.public_ip = '127.0.0.1'
    self.ip = self.public_ip

    self.connect_instructions = 'tmux a -t ' + self.tmux_session

    # task current dir
    print('name is', name)
    # tmpdir = f"{util.reverse_taskname(name)}.{os.getpid()}.{util.now_micros()}"
    launch_id = util.random_id()
    self.taskdir = f"{TASKDIR_ROOT}/{name}-{launch_id}"
    self.local_scratch = f"{SCRATCH_ROOT}/{name}-{launch_id}"
    self.remote_scratch = f"{SCRATCH_ROOT}/{name}-{launch_id}"

    self.log(f"Creating taskdir {self.taskdir}")
    self._run_raw('mkdir -p ' + self.taskdir)

    self.log(f"Creating scratch {self.local_scratch}")
    self._run_raw('rm -Rf ' + self.local_scratch)
    self._run_raw('mkdir -p ' + self.local_scratch)
    self._run_raw('mkdir -p ' + self.remote_scratch)
    self.run_counter = 0

    self._cwd = self.taskdir
    self._can_run = True
    self.run('cd ' + self.taskdir)

    print("Running install script " + install_script)
    self.install_script = install_script
    for line in install_script.split('\n'):
      self.run(line)

  def run(self, cmd, non_blocking=False, ignore_errors=False, **_kwargs):

    if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE'):
      # HACK
      if not util.is_bash_builtin(cmd) or True:
        return self._run_with_output_on_failure(cmd, non_blocking, ignore_errors, **_kwargs)
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
      return status

    cmd = cmd.strip()
    if not cmd or cmd.startswith('#'):  # ignore empty/commented out lines
      return -1
    self.run_counter += 1
    self.log("tmux> %s", cmd)

    self._cmd = cmd
    self._cmd_fn = f'{self.local_scratch}/{self.run_counter}.cmd'
    self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
    assert not os.path.exists(self._status_fn)

    cmd = util.shell_strip_comment(cmd)
    # assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    self.write(self._cmd_fn, cmd + '\n')
    modified_cmd = f'{cmd} ; echo $? > {self._status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    tmux_window = self.tmux_session+':'+str(self.tmux_window_id)
    tmux_cmd = f'tmux send-keys -t {tmux_window} {modified_cmd} Enter'
    self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
    if non_blocking:
      return 0

    if not self.wait_for_file(self._status_fn, max_wait_sec=60):
      self.log(f"Retrying waiting for {self._status_fn}")
    while not self.exists(self._status_fn):
      self.log(f"Still waiting for {cmd}")
      self.wait_for_file(self._status_fn, max_wait_sec=60)
    contents = self.read(self._status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(0.01)
    status = int(open(self._status_fn).read().strip())
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
      if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE'):
        self.log(
          f"Start failing output {extra_msg}: \n{'*'*80}\n\n '{self.read(self._out_fn)}'")
        self.log(f"\n{'*'*80}\nEnd failing output")
      if not ignore_errors:
        raise RuntimeError(f"Command {self._cmd} returned status {status}")
      else:
        self.log(f"Warning: command {self._cmd} returned status {status}")

    return status

  def switch_window(self, window_id: int):
    """
    Switches currently active tmux window for given task. 0 is the default window
    Args:
      window_id: integer id of tmux window to use
    """

    # windows are numbered sequentially 0, 1, 2, ...
    # create any missing windows and make them point to the same directory
    if window_id not in self.tmux_available_window_ids:
      for i in range(max(self.tmux_available_window_ids)+1, window_id+1):
        self._run_raw(f'tmux new-window -t {self.tmux_session} -d')

        tmux_window = self.tmux_session + ':' + str(i)
        cmd = shlex.quote(f'cd {self.taskdir}')
        tmux_cmd = f'tmux send-keys -t {tmux_window} {cmd} Enter'
        self._run_raw(tmux_cmd)
        self.tmux_available_window_ids.append(i)

    self.tmux_window_id = window_id

  # This is a future "run" command, will become "run" once all cases are checked
  def _run_with_output_on_failure(self, cmd, non_blocking=False, ignore_errors=False, **_kwargs) -> str:
    if not self._can_run:
      assert False, "Using .run before initialization finished"
    if '\n' in cmd:
      cmds = cmd.split('\n')
      self.log(
        f"Running {len(cmds)} commands at once, returning status of last")
      status = -1
      for subcmd in cmds:
        status = self.run(subcmd)
      return status

    cmd = cmd.strip()
    if not cmd or cmd.startswith('#'):  # ignore empty/commented out lines
      return ''
    self.run_counter += 1
    self.log("tmux> %s", cmd)

    self._cmd = cmd
    self._cmd_fn = f'{self.local_scratch}/{self.run_counter}.cmd'
    self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
    self._out_fn = f'{self.remote_scratch}/{self.run_counter}.out'
    assert not os.path.exists(self._status_fn)

    cmd = util.shell_strip_comment(cmd)
    # assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    self.write(self._cmd_fn, cmd + '\n')
    #  modified_cmd = f'{cmd} ; echo $? > {self._status_fn}'
    modified_cmd = f'{cmd} > >(tee -a {self._out_fn}) 2> >(tee -a {self._out_fn} >&2); echo $? > {self._status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    tmux_window = self.tmux_session+':'+str(self.tmux_window_id)
    tmux_cmd = f'tmux send-keys -t {tmux_window} {modified_cmd} Enter'
    self._run_raw(tmux_cmd)
    if non_blocking:
      return ''

    if not self.wait_for_file(self._status_fn, max_wait_sec=60):
      self.log(f"Retrying waiting for {self._status_fn}")
    while not self.exists(self._status_fn):
      self.log(f"Still waiting for {cmd}")
      self.wait_for_file(self._status_fn, max_wait_sec=60)
    contents = self.read(self._status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(0.01)
    status = int(open(self._status_fn).read().strip())
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

  def _run_raw(self, cmd, ignore_errors=False):
    """Runs command directly, skipping tmux interface"""
    # TODO: capture stdout/stderr for feature parity with aws_backend
    result = os.system(cmd)
    if result != 0:
      if ignore_errors:
        self.log(f"command ({cmd}) failed.")
        assert False, "_run_raw failed"

  def rsync(self, local_fn, remote_fn=None):
    self.upload(local_fn, remote_fn)
  
  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    """Uploads file to remote instance. If location not specified, dumps it
    into default directory. Creates missing directories in path name."""

    # support wildcard through glob
    if '*' in local_fn:
      for local_subfn in glob.glob(local_fn):
        self.upload(local_subfn)
      return

    if remote_fn is None:
      remote_fn = os.path.basename(local_fn)

    if dont_overwrite and self.exists(remote_fn):
      self.log("Remote file %s exists, skipping" % (remote_fn,))
      return

    if not remote_fn.startswith('/'):
      remote_fn = self.taskdir + '/' + remote_fn

    remote_fn = remote_fn.replace('~', self.homedir)
    self.log('uploading ' + local_fn + ' to ' + remote_fn)

    local_fn = os.path.abspath(local_fn)
    self._run_raw("cp -R %s %s" % (local_fn, remote_fn))

  def download(self, remote_fn, local_fn='.'):
    if local_fn == '.':
      local_fn = self._cwd
    #    self.log("downloading %s to %s" % (remote_fn, local_fn))
    if not remote_fn.startswith('/'):
      remote_fn = self._cwd + '/' + remote_fn
    if self.exists(remote_fn):
      os.system(f'cp {remote_fn} {local_fn}')
    else:
      raise RuntimeError(f"No such file {remote_fn}")

  def exists(self, remote_fn):
    return os.path.exists(remote_fn)

  def read(self, remote_fn):
    tmp_fn = self.local_scratch + '/' + str(util.now_micros())
    self.download(remote_fn, tmp_fn)
    return open(tmp_fn).read()

  def write(self, remote_fn, contents):
    def make_temp_fn():
      """Returns temporary filename for this task."""
      return self.local_scratch + '/write.' + str(util.now_micros())

    tmp_fn = make_temp_fn()
    open(tmp_fn, 'w').write(contents)
    self.upload(tmp_fn, remote_fn)

  # don't include file streaming for now
  # the issue is that file streaming by default turns on 4K buffering, which makes
  # streaming a lot less useful. Similar buffering is turned on for piping commands
  # https://unix.stackexchange.com/questions/25372/turn-off-buffering-in-pipe
  # def file_stream(self, fn: str) -> None:
  #   #    if not fn.startswith('/'):
  #   #      fn = self.taskdir + '/' + fn
  #
  #   if not os.path.exists(fn):
  #     os.system('mkdir -p ' + os.path.dirname(os.path.abspath(fn)))
  #     os.system('touch ' + fn)
  #
  #   p = subprocess.Popen(['tail', '-f', fn], stdout=subprocess.PIPE)
  #
  #   for line in iter(p.stdout.readline, ''):
  #     sys.stdout.write(line.decode('ascii', errors='ignore'))

  @property
  def logdir(self):
    """Returns logging directory, creating one if necessary. See "Logdir" section  of design doc on naming convention."""

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

    """Create logdir for task/job/run. No-op if the task is not chief (0'th task of 0'th job of run)
    """
    run_name = ncluster_globals.get_run_for_task(self)
    self.log("Creating logdir for run "+run_name)
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


class Job(backend.Job):
  pass


class Run:
  """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  :ivar aws_placement_group_name: somedoc
  """
  jobs: List[Job]

  def __init__(self, name='', **kwargs):
    """Creates a run. If install_script is specified, it's used as default
    install_script for all jobs (can be overridden by Job constructor)"""

    self.name = name
    self.kwargs = kwargs

  @property
  def logdir(self):
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

  def rsync(self, *args, **kwargs):
    """Runs command on every job in the run."""
    for job in self.jobs:
      job.rsync(*args, **kwargs)

  def make_job(self, name='', **kwargs):
    return make_job(name+'.'+self.name, run_name=self.name, **kwargs)


def make_task(name='',
              run_name='',
              **kwargs) -> Task:
  """Create task, also create dummy run if not specified."""
  ncluster_globals.task_launched = True

  name = ncluster_globals.auto_assign_task_name_if_needed(name)

  # tmux can't use . for session names
  tmux_session = name.replace('.', '=')
  tmux_window_id = 0
  util.log(f'killing session {tmux_session}')

  if not util.is_set("NCLUSTER_NOKILL_TMUX"):
    os.system(f'tmux kill-session -t {tmux_session}')
  os.system(f'tmux new-session -s {tmux_session} -n {tmux_window_id} -d')

  task = Task(name,
              tmux_session=tmux_session,  # propagate optional args
              run_name=run_name,
              **kwargs)
  ncluster_globals.register_task(task, run_name)
  return task


def make_job(name="",
             num_tasks=1,
             run_name="",
             install_script='',
             **kwargs
             ) -> backend.Job:
  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"

  name = ncluster_globals.auto_assign_job_name_if_needed(name)
  util.validate_ncluster_job_name(name)
  tasks = [make_task(f"{i}.{name}",
                     run_name=run_name,
                     install_script=install_script,
                     **kwargs
                     ) for i in range(num_tasks)]

  job = backend.Job(name=name, tasks=tasks, **kwargs)
  return job


def make_run(name) -> Run:
  run = Run(name)
  return run
