from . import aws_backend
from . import aws_util
from . import util

from .aws_backend import make_task
from .aws_backend import make_job

# for type annotations
from .aws_backend import Job
from .aws_backend import Task

from .aws_util import running_on_aws
from .aws_util import get_zone
from .aws_util import get_region

from ._version import __version__

from .aws_backend import make_job
from .aws_backend import make_task

from . import aws_util as u

from . import ncluster_globals


def add_authorized_keys(keys: list) -> str:
  """Add given public keys to list of keys authorized to access ncluster instances."""

  current_keys = os.environ.get('NCLUSTER_AUTHORIZED_KEYS', '')
  if current_keys:
    current_keys_list = current_keys.split(';')
  else:
    current_keys_list = []

  current_keys_list.extend(keys)
  current_keys_str = ';'.join(current_keys_list)
  os.environ['NCLUSTER_AUTHORIZED_KEYS'] = current_keys_str
  return current_keys_str


# # TODO: remove?
# import collections
# def join(things_to_join):
#   if isinstance(things_to_join, collections.Iterable):
#     for thing in things_to_join:
#       thing.join()
#   else:
#     things_to_join.join()

print(f"ncluster version {__version__}")

if not util.is_set('NCLUSTER_DISABLE_PDB_HANDLER'):
  util.install_pdb_handler()  # CTRL+\ drops into pdb

