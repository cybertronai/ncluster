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


print(f"ncluster version {__version__}")

if not util.is_set('NCLUSTER_DISABLE_PDB_HANDLER') and not util.is_set('NCLUSTER_RUNNING_UNDER_CIRCLECI'):
  util.install_pdb_handler()  # CTRL+\ drops into pdb

