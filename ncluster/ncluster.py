from . import aws_backend
from . import local_backend
from . import backend  # TODO: remove?

import collections

_backend: backend = None


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


def make_run(name='', **kwargs) -> backend.Run:
  return _backend.make_run(name, **kwargs)


def make_job(name='', **kwargs) -> backend.Job:
  return _backend.make_job(name, **kwargs)


def make_task(name='', **kwargs) -> backend.Task:
  return _backend.make_task(name, **kwargs)


def join(things_to_join):
  if isinstance(things_to_join, collections.Iterable):
    for thing in things_to_join:
      thing.join()
  else:
    things_to_join.join()
