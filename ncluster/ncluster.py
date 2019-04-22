from . import aws_backend
from . import local_backend
from . import backend
from . import aws_util as u
import collections

from . import ncluster_globals

_backend: type(backend) = backend


def get_logdir_root() -> str:
  return ncluster_globals.LOGDIR_ROOT


def set_logdir_root(logdir_root):
  """Globally changes logdir root for all runs."""
  ncluster_globals.LOGDIR_ROOT = logdir_root


def set_backend(backend_name: str):
  """Sets backend (local or aws)"""
  global _backend, _backend_name
  _backend_name = backend_name

  assert not ncluster_globals.task_launched, "Not allowed to change backend after launching a task (this pattern is error-prone)"
  if backend_name == 'aws':
    _backend = aws_backend
  elif backend_name == 'local':
    _backend = local_backend
  else:
    assert False, f"Unknown backend {backend_name}"

  # take default value for logdir root from backend
  ncluster_globals.LOGDIR_ROOT = _backend.DEFAULT_LOGDIR_ROOT


def use_aws():
  set_backend('aws')


def use_local():
  set_backend('local')


def get_backend() -> str:
  """Returns backend name, ie "local" or "aws" """
  return _backend_name


def get_backend_module() -> backend:
  return _backend


def running_locally():
  return get_backend() == 'local'


def get_region() -> str:
  if _backend != local_backend:
    return u.get_region()
  else:
    return 'local'


def get_zone() -> str:
  if _backend != local_backend:
    return u.get_zone()
  else:
    return 'local'


#  def make_run(name='', **kwargs):
#  return _backend.Run(name, **kwargs)


# Use factory methods task=create_task instead of relying solely on constructors task=Task() because underlying hardware resources may be reused between instantiations
# For instance, one may create a Task initialized with an instance that was previous created for this kind of task
# Factory method will make the decision to recreate or reuse such resource, and wrap this resource with a Task object.
def make_task(name: str = '',
              run_name: str = '',
              install_script: str = '',
              **kwargs) -> backend.Task:
  return _backend.make_task(name=name, run_name=run_name,
                            install_script=install_script, **kwargs)


def make_job(name: str = '',
             run_name: str = '',
             num_tasks: int = 0,
             install_script: str = '',
             **kwargs
             ) -> backend.Job:
  """
  Create a job using current backend. Blocks until all tasks are up and initialized.

  Args:
    name: name of the job
    run_name: name of the run (auto-assigned if empty)
    num_tasks: number of tasks
    install_script: bash-runnable script
    **kwargs:

  Returns:
    backend.Job
  """
  return _backend.make_job(name=name, run_name=run_name, num_tasks=num_tasks,
                           install_script=install_script, **kwargs)


def make_run(name: str = '', **kwargs) -> backend.Run:
  return _backend.make_run(name=name, **kwargs)


# TODO: remove?
def join(things_to_join):
  if isinstance(things_to_join, collections.Iterable):
    for thing in things_to_join:
      thing.join()
  else:
    things_to_join.join()
