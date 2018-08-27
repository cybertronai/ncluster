"""Interface for job launching backend."""
# Job launcher Python API: https://docs.google.com/document/d/1yTkb4IPJXOUaEWksQPCH7q0sjqHgBf3f70cWzfoFboc/edit
# AWS job launcher (concepts): https://docs.google.com/document/d/1IbVn8_ckfVO3Z9gIiE0b9K3UrBRRiO9HYZvXSkPXGuw/edit

import threading
import time

# aws_backend.py
# local_backend.py

LOGDIR_PREFIX = '/efs/runs'

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


def _set_global_logdir_prefix(logdir_prefix):
  """Globally changes logdir prefix across all runs."""
  global LOGDIR_PREFIX
  LOGDIR_PREFIX = logdir_prefix


def _current_timestamp():
  # timestamp format from https://github.com/tensorflow/tensorflow/blob/155b45698a40a12d4fef4701275ecce07c3bb01a/tensorflow/core/platform/default/logging.cc#L80
  current_seconds = time.time()
  remainder_micros = int(1e6 * (current_seconds - int(current_seconds)))
  time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_seconds))
  full_time_str = "%s.%06d" % (time_str, remainder_micros)
  return full_time_str


class Run:
  """Run is a collection of jobs that share statistics. IE, training run will contain gradient worker job,
  parameter server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store
  checkpoints and event files. """

  def __init__(self, name, install_script=None):
    """Creates a run. If install_script is specified, it's used as default
    install_script for all jobs (can be overridden by Job constructor)"""
    self.jobs = []
    self.name = ''
    raise NotImplementedError()

  def make_job(self, name, num_tasks=1, install_script=None, **kwargs):
    """Creates job in the given run. If install_script is None, uses
    install_script associated with the Run."""
    raise NotImplementedError()

  def run(self, *args, **kwargs):
    """Runs command on every job in the run."""

    for job in self.jobs:
      job.run(*args, **kwargs)

  def _run_and_capture_output(self, *args, **kwargs):
    """Runs command on every first job in the run, returns stdout."""

    return self.jobs[0]._run_and_capture_output(*args, **kwargs)

  def _run_raw(self, *args, **kwargs):
    """_run_raw on every job in the run."""

    for job in self.jobs:
      job._run_raw(*args, **kwargs)

  def upload(self, *args, **kwargs):
    """Runs command on every job in the run."""

    for job in self.jobs:
      job.upload(*args, **kwargs)

  def log(self, message, *args):
    """Log to client console."""
    ts = _current_timestamp()
    if args:
      message = message % args

    print("%s %s: %s" % (ts, self.name, message))


class Job:
  def __init__(self, name, tasks, **kwargs):
    self.name = name
    self.tasks = tasks
    self.kwargs = kwargs
    for task in tasks:
      task.job = self

  def _async_wrapper(self, method, *args, **kwargs):
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
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    if exceptions: raise exceptions[0]

  def run(self, *args, **kwargs):
    """Runs command on every task in the job in parallel, blocks until all tasks finish.
    See Task for documentation of args/kwargs."""
    return self._async_wrapper("run", *args,**kwargs)

  def upload(self, *args, **kwargs):
    return self._async_wrapper("upload", *args,**kwargs)

  def file_write(self, *args, **kwargs):
    return self._async_wrapper("file_write", *args,**kwargs)


class Task:
  def __init__(self):
    self.name = None
    self.instance = None
    self.install_script = None
    self.job = None
    self.kwargs = None

    self.public_ip = None
    self.ip = None

  def run(self, cmd, async, ignore_errors):
    """Runs command on given task."""
    raise NotImplementedError()

  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    """Uploads given file to the task. If remote_fn is not specified, dumps it
    into task current directory with the same name."""
    raise NotImplementedError()

  def download(self, remote_fn, local_fn=None):
    """Downloads remote file to current directory."""
    raise NotImplementedError()

  def file_write(self, fn, contents):
    """Write string contents to file fn in task."""
    raise NotImplementedError()

  def file_read(self, fn):
    """Read contents of file and return it as string."""
    raise NotImplementedError()

  def file_exists(self, fn):
    """Return true if file exists in task current directory."""
    raise NotImplementedError()


  def _run_raw(self, cmd):
    """Runs command directly on every task in the job, skipping tmux interface. Use if want to create/manage additional tmux sessions manually."""
    raise NotImplementedError()

  def _log(self, message, *args):
    """Log to launcher console."""
    if args:
      message = message % args

    print(f"{_current_timestamp()} {self.name}: {message}")


# Use factory methods task=create_task instead of relying solely on constructors task=Task() because underlying hardware resources may be reused between instantiations
# For instance, one may create a Task initialized with an instance that was previous created for this kind of task
# Factory method will make the decision to recreate or reuse such resource, and wrap this resource with a Task object.

def make_job() -> Job:
  pass


def make_task() -> Task:
  pass


# todo: rename to "start_run" instead of setup_run?
def make_run(name) -> Run:
  """Sets up "run" with given name, such as "training run"."""
  raise NotImplementedError()