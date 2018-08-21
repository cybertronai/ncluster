import os
from . import aws_backend
from . import tmux_backend

def set_backend(backend_name):
  """Sets backend (local or aws)"""
  global _backend
  if backend_name == 'aws':
    _backend = aws_backend
  elif backend_name == 'local':
    _backend = tmux_backend
  else:
    assert False, f"Unknown backend {backend_name}"

def make_run(name, **kwargs):
  return _backend.Run(name, **kwargs)

def make_job(name, **kwargs):
  return _backend.make_job(name, **kwargs)

def make_task(name, **kwargs):
  return _backend.make_task(name, **kwargs)

def join(things_to_join):
  for thing in things_to_join:
    thing.join()

# set default backend from environment
if 'NCLUSTER_BACKEND' in os.environ:
  set_backend(os.environ['NCLUSTER_BACKEND'])
else:
  set_backend('aws')
  
_backend = None


