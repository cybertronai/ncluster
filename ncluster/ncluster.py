from . import aws_backend
from . import local_backend
from . import backend  # TODO: remove?
from . import util
from . import aws_util as u

util.install_pdb_handler()  # CTRL+\ drops into pdb

import collections

_backend: type(backend) = backend


def set_backend(backend_name: str):
  """Sets backend (local or aws)"""
  global _backend, _backend_name
  _backend_name = backend_name
  if backend_name == 'aws':
    _backend = aws_backend
  elif backend_name == 'local':
    _backend = local_backend
  else:
    assert False, f"Unknown backend {backend_name}"


def get_backend() -> str:
  """Returns backend name, ie "local" or "aws" """
  return _backend_name


def get_backend_module() -> backend:
  return _backend


#  def make_run(name='', **kwargs):
#  return _backend.Run(name, **kwargs)


# Use factory methods task=create_task instead of relying solely on constructors task=Task() because underlying hardware resources may be reused between instantiations
# For instance, one may create a Task initialized with an instance that was previous created for this kind of task
# Factory method will make the decision to recreate or reuse such resource, and wrap this resource with a Task object.
def make_task(name: str = '',
              run_name: str = '',
              install_script: str = '',
              **kwargs) -> backend.Task:
  return _backend.make_task(name=name, run_name=run_name, install_script=install_script, **kwargs)


def make_job(name: str = '',
             run_name: str = '',
             num_tasks: int = 0,
             install_script: str = '',
             **kwargs
             ) -> backend.Job:
  return _backend.make_job(name, run_name, num_tasks, install_script, **kwargs)


def make_run(name: str = '', **kwargs) -> backend.Run:
  return _backend.make_run(name, **kwargs)


def get_logdir_root():
  return _backend.get_logdir_root()


def set_global_logdir_root(logdir_root):
  return _backend.set_global_logdir_root(logdir_root)


def get_zone():
  if _backend != local_backend:
    return u.get_zone()
  else:
    return 'local'

# TODO: remove?
def join(things_to_join):
  if isinstance(things_to_join, collections.Iterable):
    for thing in things_to_join:
      thing.join()
  else:
    things_to_join.join()
