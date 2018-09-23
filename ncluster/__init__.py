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


# set default backend from environment
if 'NCLUSTER_BACKEND' in os.environ:
  set_backend(os.environ['NCLUSTER_BACKEND'])
else:
  set_backend('local')

util.install_pdb_handler()  # CTRL+\ drops into pdb
