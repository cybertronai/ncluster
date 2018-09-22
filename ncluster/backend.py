"""Interface for job launching backend."""
# Job launcher Python API: https://docs.google.com/document/d/1yTkb4IPJXOUaEWksQPCH7q0sjqHgBf3f70cWzfoFboc/edit
# AWS job launcher (concepts): https://docs.google.com/document/d/1IbVn8_ckfVO3Z9gIiE0b9K3UrBRRiO9HYZvXSkPXGuw/edit
import os
import sys
import threading
import time
from typing import List, Tuple, Any

from . import ncluster_globals
from . import util

# aws_backend.py
# local_backend.py

LOGDIR_ROOT: str = None  # location of logdir for this backend

"""
backend = aws_backend # alternatively, backend=tmux_backend to launch jobs locally in separate tmux sessions
run = backend.make_run("helloworld")  # sets up /efs/runs/helloworld
worker_job = run.make_job("worker", instance_type="g3.4xlarge", num_tasks=4, ami=ami, setup_script=setup_script)
ps_job = run.make_job("ps", instance_type="c5.xlarge", num_tasks=4, ami=ami, setup_script=setup_script)
setup_tf_config(worker_job, ps_job)
ps_job.run("python cifar10_main.py --num_gpus=0")  # runs command on each task
worker_job.run("python cifar10_main.py --num_gpus=4")

tb_job = run.make_job("tb", instance_type="m4.xlarge", num_tasks=1, public_port=6006)
tb_job.run("tensorboard --logdir=%s --port=%d" %(run.logdir, 6006))
# when job has one task, job.task[0].ip can be accessed as job.ip
print("See TensorBoard progress on %s:%d" %(tb_job.ip, 6006))
print("To interact with workers: %s" %(worker_job.connect_instructions))


To reconnect to existing job:

"""


class Task:
  name: str
  ip: str
  public_ip: str
  run_counter: int
  # location where temporary files from interfacing with task go locally
  local_scratch: str
  # location where temporary files from interfacing with task go on task
  remote_scratch: str
  job: Any  # can't declare Job because of circular dependency

  def __init__(self, name=''):
    """Wraps execution resources into a task. Runs install_script if present"""
    self.name = name
    self.instance = None
    self.install_script = None
    self.job = None
    self.kwargs = None
    self.public_ip = None
    self.ip = None
    self.logdir_ = None

  @property
  def logdir(self):
    self.setup_logdir()  # creates logdir if necessary, stores it in associated run_.logdir_
    return self.job.run_.logdir_

  @property
  def run_name(self):
    return self.job.run_.name

  def is_chief(self):
    """Tests whether this task is 'chief', in other words, in charge of creating shared tasks for this
    run, such as creating logging directory."""

    if not self.job:  # standalone task, always chief
      return True
    else:  # only first task in first job is chielf
      return self.job.tasks.index(self) == 0 and self.job.is_chief()

  # TODO: this should be marked private
  def setup_logdir(self):
    """Create logdir for task/job/run. No-op if the task is not chief (0'th task of 0'th job of run)
    """
    assert self.job.run_.name

    if not self.is_chief():
      return
    if self.job.run_.logdir_:
      return  # already created logdir

    self.log("Creating logdir")

    # logdir root can differ between backends, hence get it from the actual backend being used
    logdir_root = ncluster_globals.LOGDIR_ROOT
    assert logdir_root

    self.run(f'mkdir -p {logdir_root}')
    find_command = f'find {logdir_root} -maxdepth 1 -type d'

    stdout, stderr = self.run_with_output(find_command)
    logdir = f"{logdir_root}/{self.run_name}"

    # TODO, simplify this logic, just get the largest logdir encountered, then do +1
    counter = 0
    while logdir in stdout:
      counter += 1
      lll = f'{logdir_root}/{self.job.run_.name}.{counter:02d}'
      self.log(f'Warning, logdir {logdir} exists, deduping to {lll}')
      logdir = lll

    # fix permission for all parent directories.
    # EFS may be shared among various users (ie ubuntu and ec2-user), so make
    # sure all parent dirs are world-writeable
    fragments = logdir.split('/')[1:]
    root = ''
    for fragment in fragments:
      root = root + '/' + fragment
      self.run('sudo chmod 777 ' + root, ignore_errors=True)

    self.run(f'mkdir -p {logdir}')
    self.job.run_.logdir_ = logdir

  def run(self, cmd: str, non_blocking=False, ignore_errors=False):
    """Runs command on given task."""
    raise NotImplementedError()

  def run_with_output(self, cmd, non_blocking=False, ignore_errors=False) -> Tuple[str, str]:
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
    stdout = self.file_read(stdout_fn)
    stderr = self.file_read(stderr_fn)

    if status > 0:
      self.log(f"Warning: command '{cmd}' returned {status},"
               f" stdout was '{stdout}' stderr was '{stderr}'")
      if not ignore_errors:
        raise RuntimeError(f"Warning: command '{cmd}' returned {status},"
                           f" stdout was '{stdout}' stderr was '{stderr}'")

    return stdout, stderr

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
    print("Waiting for file", fn)
    start_time = time.time()
    while True:
      if time.time() - start_time > max_wait_sec:
        util.log(f"Timeout exceeded ({max_wait_sec} sec) for {fn}")
        return False
      if not self.file_exists(fn):
        time.sleep(check_interval)
        continue
      else:
        break
    return True

  def _run_raw(self, cmd):
    """Runs command directly on every task in the job, skipping tmux interface. Use if want to create/manage additional tmux sessions manually."""
    raise NotImplementedError()

  def upload(self, local_fn: str, remote_fn: str = '',
             dont_overwrite: bool = False):
    """Uploads given file to the task. If remote_fn is not specified, dumps it
    into task current directory with the same name.

    Args:
      local_fn: location of file locally
      remote_fn: location of file on task
      dont_overwrite: if True, will be no-op if target file exists
      """
    raise NotImplementedError()

  def download(self, remote_fn: str, local_fn: str = ''):
    """Downloads remote file to current directory."""
    raise NotImplementedError()

  def file_write(self, fn, contents):
    """Write string contents to file fn in task."""
    raise NotImplementedError()

  def file_read(self, fn):
    """Read contents of file and return it as string."""
    raise NotImplementedError()

  def file_exists(self, fn) -> bool:
    """Checks if fn exists on task

    Args:
      fn: filename local to task
    Returns:
      true if fn exists on task machine
    """
    raise NotImplementedError()

  def log(self, message, *args):
    """Log to launcher console."""
    if args:
      message %= args

    print(f"{util.current_timestamp()} {self.name}: {message}")


class Job:
  name: str
  tasks: List[Task]

  #  run_: Run

  def __init__(self, name: str, run_, tasks: List[Task] = None, **kwargs):
    """Initializes Job object, links tasks to refer back to the Job."""
    if tasks is None:
      tasks = []
    self.name = name
    self.run_ = run_
    self.tasks = tasks
    self.kwargs = kwargs
    # TODO: maybe backlinking is not needed
    for task in tasks:
      task.job = self

  @property
  def logdir(self):
    return self.tasks[0].logdir

  def is_chief(self):
    """Return true if this task is first task in the Run"""
    if not self.run_:  # standalone Job
      return True
    return self.run_.jobs.index(self) == 0

  def _non_blocking_wrapper(self, method, *args, **kwargs):
    """Runs given method on every task in the job. Blocks until all tasks finish. Propagates exception from first
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
      thread.join()
    if exceptions:
      raise exceptions[0]

  def run(self, *args, **kwargs):
    """Runs command on every task in the job in parallel, blocks until all tasks finish.
    See Task for documentation of args/kwargs."""
    return self._non_blocking_wrapper("run", *args, **kwargs)

  def run_with_output(self, *args, **kwargs):
    """Runs command on every task in the job in parallel, blocks until all tasks finish.
    See Task for documentation of args/kwargs."""
    return self._non_blocking_wrapper("run_with_output", *args, **kwargs)

  def upload(self, *args, **kwargs):
    """See :py:func:`backend.Task.upload`"""
    return self._non_blocking_wrapper("upload", *args, **kwargs)

  def file_write(self, *args, **kwargs):
    return self._non_blocking_wrapper("file_write", *args, **kwargs)

  def _run_raw(self, *args, **kwargs):
    return self._non_blocking_wrapper("_run_raw", *args, **kwargs)


class Run:
  """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  :ivar aws_placement_group_name: somedoc
  """
  aws_placement_group_name: str  # This is unique name used to place all tasks in the run together
  jobs: List[Job]

  def __init__(self, name='', jobs=None, **kwargs):
    """Creates a run. If install_script is specified, it's used as default
    install_script for all jobs (can be overridden by Job constructor)"""

    util.log(f"Creating run '{name}'")
    if not name:
      # use directory/filename for run name
      main_script = os.path.abspath(sys.argv[0])
      if main_script.find('/') == 1:
        main_script = 'asdf/' + main_script
      filename = '.'.join(main_script.rsplit('/', 2)[-2:])
      name = f'unnamedrun-{filename}'

    if jobs is None:
      jobs = []
    self.name = name
    self.jobs = jobs
    self.kwargs = kwargs

    self.logdir_ = None
    util.log(f"Choosing placement_group for run {name}")
    self.aws_placement_group_name = name + '-' + util.random_id()

    # TODO: this back-linking logic may be unneeded
    for job in jobs:
      job.run_ = self

  @property
  def logdir(self):
    assert self.jobs
    return self.jobs[0].logdir

  def make_job(self, name='', **kwargs):
    return Job(name, self, **kwargs)

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

  # def log(self, message, *args):
  #   """Log to client console."""
  #   ts = _current_timestamp()
  #   if args:
  #     message = message % args
  #
  #   print("%s %s: %s" % (ts, self.name, message))


def make_task(**_kwargs) -> Task:
  raise NotImplementedError()


def make_job(**_kwargs) -> Job:
  raise NotImplementedError()


def make_run(**_kwargs) -> Run:
  raise NotImplementedError()
