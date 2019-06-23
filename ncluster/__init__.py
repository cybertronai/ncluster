import os

from . import aws_backend
from . import aws_util
from . import util
from . import local_backend
from . import backend  # TODO: remove?

from .ncluster import get_backend
from .ncluster import set_backend
from .ncluster import running_locally

from .ncluster import use_aws
from .ncluster import use_local

from .ncluster import make_task
from .ncluster import make_job
from .ncluster import make_run
from .ncluster import get_zone
from .ncluster import get_region
from .ncluster import set_logdir_root
from .ncluster import get_logdir_root
from .ncluster import add_authorized_keys

# set default backend from environment
if 'NCLUSTER_BACKEND' in os.environ:
  set_backend(os.environ['NCLUSTER_BACKEND'])
else:
  set_backend('aws')

# whitelist of settings customizable through env vars. Should keep this small to avoid complexity.
ncluster_env_whitelist = {'NCLUSTER_IMAGE', 'NCLUSTER_AWS_FAST_ROOTDISK', 'NCLUSTER_ZONE',
                          'NCLUSTER_AUTHORIZED_KEYS', 'NCLUSTER_AWS_PLACEMENT_GROUP', 'NCLUSTER_DISABLE_PDB_HANDLER'}

# print/validate custom settings
for v in os.environ:
  if v.startswith('NCLUSTER'):
    assert v in ncluster_env_whitelist, f"Custom setting '{v}'='{os.environ[v]}' not in ncluster_env_whitelist, if you" \
      f"are sure you need this setting, add it to the whitelist in __init__.py, otherwise 'unset {v}'"
    if v == 'NCLUSTER_AUTHORIZED_KEYS':
      continue  # don't spam console since this is often set by default
    print(f"ncluster env setting {v}={os.environ[v]}")

if not util.is_set('NCLUSTER_DISABLE_PDB_HANDLER'):
  util.install_pdb_handler()  # CTRL+\ drops into pdb
