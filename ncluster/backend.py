"""Interface for job launching backend.

Run/Job and Task are container classes encapsulating functionality.
User creates them through make_run/make_job/make_task methods

"""
# Job launcher Python API: https://docs.google.com/document/d/1yTkb4IPJXOUaEWksQPCH7q0sjqHgBf3f70cWzfoFboc/edit
# AWS job launcher (concepts): https://docs.google.com/document/d/1IbVn8_ckfVO3Z9gIiE0b9K3UrBRRiO9HYZvXSkPXGuw/edit
import threading
import time
from typing import List, Tuple, Any

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
    self.last_status = None
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
    raise NotImplementedError()

  def run(self, cmd: str, non_blocking=False, ignore_errors=False):
    """Runs command on given task."""
    raise NotImplementedError()

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
      if not self.exists(fn):
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

  def write(self, fn, contents):
    """Write string contents to file fn in task."""
    raise NotImplementedError()

  def read(self, fn):
    """Read contents of file and return it as string."""
    raise NotImplementedError()

  def exists(self, fn) -> bool:
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

  
  def rsync(self, *args, **kwargs):
    """See :py:func:`backend.Task.rsync`"""
    return self._non_blocking_wrapper("rsync", *args, **kwargs)

  def upload(self, *args, **kwargs):
    """See :py:func:`backend.Task.upload`"""
    return self._non_blocking_wrapper("upload", *args, **kwargs)

  def write(self, *args, **kwargs):
    return self._non_blocking_wrapper("write", *args, **kwargs)

  def _run_raw(self, *args, **kwargs):
    return self._non_blocking_wrapper("_run_raw", *args, **kwargs)


# Implementation needs to be backend specific so that run.create_job calls backend-specific method
class Run:
  """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  :ivar aws_placement_group_name: somedoc
  """
  jobs: List[Job]

  @property
  def logdir(self):
    raise NotImplementedError()

  # TODO: currently this is synchronous, use non_blocking wrapper like in Job to parallelize methods
  def run(self, *args, **kwargs):
    raise NotImplementedError()

  def run_with_output(self, *args, **kwargs):
    raise NotImplementedError()

  def _run_raw(self, *args, **kwargs):
    raise NotImplementedError()

  def upload(self, *args, **kwargs):
    raise NotImplementedError()

  def make_job(self, name='', **kwargs):
    raise NotImplementedError()


def make_task(**_kwargs):
  raise NotImplementedError()


def make_job(**_kwargs):
  raise NotImplementedError()


def make_run(**_kwargs):
  raise NotImplementedError()
