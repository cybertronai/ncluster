"""
Various helper utilities used internally by ncluster project, that are not explicitly tied to AWS
"""

import os
import random
import string
import sys
import time
from collections import Iterable
import shlex

from typing import Optional

import portalocker

# starting value for now_micros (Aug 31, 2018), using this to make various timestamped names shorter
EPOCH_MICROS = 1535753974788163


# Whitelist of temporary settings customizable through env vars. These are work-arounds for issues that are
# don't have permanent solutions yet. Keep this small to avoid many moving parts.
env_settings = {
  'NCLUSTER_AUTHORIZED_KEYS',         # public keys used to authorize ssh access on all instances
  'NCLUSTER_AWS_FAST_ROOTDISK',       # request $1/hour high performance AWS disk
  'NCLUSTER_AWS_PLACEMENT_GROUP',     # name of placement group to use, use when adding machines to a previous launched job
  'NCLUSTER_DISABLE_PDB_HANDLER',     # don't intercept pdb exception by default
  #  'NCLUSTER_IMAGE',
  'NCLUSTER_SSH_USERNAME',            # used as workaround when Amazon Linux detection fails
  'NCLUSTER_ZONE',                    # zone spec for when automatic zone fails (p3dn's + spot instances)
  'NCLUSTER_AWS_FORCE_CREATE_RESOURCES',  # AWS resources are created ignoring automatic existence checks
}


# keep this here instead of aws_backend because it's used by its dependency aws_util
VALID_REGIONS = ['us-east-2',
                 'us-east-1',
                 'us-west-1',       # An error occurred (Unsupported) when calling the RunInstances operation
                 'us-west-2',
                 'ap-east-1',       # doesn't have ec2
                 'ap-south-1',      # no EFS
                 'ap-northeast-3',  # An error occurred (OptInRequired) when calling the DescribeVpcs operation
                 'ap-northeast-2',
                 'ap-southeast-1',
                 'ap-southeast-2',
                 'ap-northeast-1',
                 'ca-central-1',
                 'cn-north-1',      # account number
                 'cn-northwest-1',  # account number
                 'eu-central-1',
                 'eu-west-1',
                 'eu-west-2',
                 'eu-west-3',       # no EFS
                 'eu-north-1',      # no EFS
                 'sa-east-1',       # no EFS
                 'us-gov-east-1',   # not authorized
                 'us-gov-west-1',   # not authorized
                 ]

# print/validate custom settings
for v in os.environ:
  if v.startswith('NCLUSTER'):
    assert v in env_settings, f"Custom setting '{v}'='{os.environ[v]}' not in settings whitelist, if you" \
      f"are sure you need this setting, add it to the env_settings in {os.path.basename(__file__)}, otherwise 'unset {v}'"
    # the following are often set by default, so don't print them
    if v in {'NCLUSTER_AUTHORIZED_KEYS', 'NCLUSTER_ZONE'}:
      continue

    sys.stderr.write(f"ncluster env setting {v}={os.environ[v]}\n")


def is_iterable(k):
  return isinstance(k, Iterable)


def now_micros(absolute=False) -> int:
  """Return current micros since epoch as integer."""
  micros = int(time.time() * 1e6)
  if absolute:
    return micros
  return micros - EPOCH_MICROS


def now_millis(absolute=False) -> int:
  """Return current millis since epoch as integer."""
  millis = int(time.time() * 1e3)
  if absolute:
    return millis
  return millis - EPOCH_MICROS // 1000


def current_timestamp() -> str:
  # timestamp format from https://github.com/tensorflow/tensorflow/blob/155b45698a40a12d4fef4701275ecce07c3bb01a/tensorflow/core/platform/default/logging.cc#L80
  current_seconds = time.time()
  remainder_micros = int(1e6 * (current_seconds - int(current_seconds)))
  time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_seconds))
  full_time_str = "%s.%06d" % (time_str, remainder_micros)
  return full_time_str


def log_error(*args, **kwargs):
  print(f"Error encountered {args} {kwargs}")


def log(*args, **kwargs):
  print(f"{args} {kwargs}")


def install_pdb_handler():
  """Automatically start pdb:
      1. CTRL+\\ breaks into pdb.
      2. pdb gets launched on exception.
  """

  import signal
  import pdb

  def handler(_signum, _frame):
    pdb.set_trace()
  signal.signal(signal.SIGQUIT, handler)

  # Drop into PDB on exception
  # from https://stackoverflow.com/questions/13174412
  def info(type_, value, tb):
   if hasattr(sys, 'ps1') or not sys.stderr.isatty():
      # we are in interactive mode or we don't have a tty-like
      # device, so we call the default hook
      sys.__excepthook__(type_, value, tb)
   else:
      import traceback
      import pdb
      # we are NOT in interactive mode, print the exception...
      traceback.print_exception(type_, value, tb)
      print()
      # ...then start the debugger in post-mortem mode.
      pdb.pm()

  sys.excepthook = info


def shell_add_echo(script):
  """Goes over each line script, adds "echo cmd" in front of each cmd.

  ls a

  becomes

  echo * ls a
  ls a
  """
  new_script = ""
  for cmd in script.split('\n'):
    cmd = cmd.strip()
    if not cmd:
      continue
    new_script += "echo \\* " + shlex.quote(cmd) + "\n"
    new_script += cmd + "\n"
  return new_script


def shell_strip_comment(cmd):
  """ hi # testing => hi"""
  if '#' in cmd:
    return cmd.split('#', 1)[0]
  else:
    return cmd


def random_id(k=5):
  """Random id to use for AWS identifiers."""
  #  https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
  return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))


def alphanumeric_hash(s: str, size=5):
  """Short alphanumeric string derived from hash of given string"""
  import hashlib
  import base64
  hash_object = hashlib.md5(s.encode('ascii'))
  s = base64.b32encode(hash_object.digest())
  result = s[:size].decode('ascii').lower()
  return result


def reverse_taskname(name: str) -> str:
  """
  Reverses components in the name of task. Reversed convention is used for filenames since
  it groups log/scratch files of related tasks together

  0.somejob.somerun -> somerun.somejob.0
  0.somejob -> somejob.0
  somename -> somename

  Args:
    name: name of task

  """
  components = name.split('.')
  assert len(components) <= 3
  return '.'.join(components[::-1])


def is_bash_builtin(cmd):
  """Return true if command is invoking bash built-in
  """
  # from compgen -b
  bash_builtins = ['alias', 'bg', 'bind', 'alias', 'bg', 'bind', 'break',
                   'builtin', 'caller', 'cd', 'command', 'compgen', 'complete',
                   'compopt', 'continue', 'declare', 'dirs', 'disown', 'echo',
                   'enable', 'eval', 'exec', 'exit', 'export', 'false', 'fc',
                   'fg', 'getopts', 'hash', 'help', 'history', 'jobs', 'kill',
                   'let', 'local', 'logout', 'mapfile', 'popd', 'printf',
                   'pushd', 'pwd', 'read', 'readarray', 'readonly', 'return',
                   'set', 'shift', 'shopt', 'source', 'suspend', 'test',
                   'times', 'trap', 'true', 'type', 'typeset', 'ulimit',
                   'umask', 'unalias', 'unset', 'wait']
  toks = cmd.split()
  if toks and toks[0] in bash_builtins:
    return True
  return False


def is_set(name: str) -> bool:
  """Helper method to check if given property is set"""
  assert name in env_settings

  val = os.environ.get(name, '0')
  assert val == '0' or val == '1', f"env var {name} has value {val}, expected 0 or 1"
  return val == '1'


def get_env(name: str) -> Optional[str]:
  """Helper method to retrieve custom env setting, returns None if not set"""
  assert name in env_settings
  return os.environ.get(name, None)


def set_env(name: str, value: str) -> None:
  """Helper method to set custom env setting"""
  assert name in env_settings
  os.environ[name] = value


def assert_script_in_current_directory():
  """Assert fail if current directory is different from location of the script"""

  script = sys.argv[0]
  assert os.path.abspath(os.path.dirname(script)) == os.path.abspath(
    '.'), f"Change into directory of script {script} and run again."


def validate_ncluster_job_name(name):
  assert name.count(
    '.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"


def toseconds(dt) -> float:
  """Converts datetime object to seconds."""
  return time.mktime(dt.utctimetuple())


def wait_for_file(fn: str, max_wait_sec: int = 60,
                  check_interval: float = 1) -> bool:
    """
    Waits for file maximum of max_wait_sec. Returns True if file was detected within specified max_wait_sec
    Args:
      fn: filename
      max_wait_sec: how long to wait in seconds
      check_interval: how often to check in seconds
    Returns:
      False if waiting was was cut short by max_wait_sec limit, True otherwise
    """
    log("Waiting for file", fn)
    start_time = time.time()
    while True:
      if time.time() - start_time > max_wait_sec:
        log(f"Timeout exceeded ({max_wait_sec} sec) for {fn}")
        return False
      if not os.path.exists(fn):
        time.sleep(check_interval)
        continue
      else:
        break
    return True


# locations of default keypair
ID_RSA = os.environ['HOME'] + '/.ssh/id_rsa'
ID_RSA_PUB = ID_RSA + '.pub'


def setup_local_ssh_keys() -> str:
  """Sanity checks on local ssh keypair and regenerate it if necessary. Returns location of public keypair file"""

  if os.path.exists(ID_RSA_PUB):
    assert os.path.exists(ID_RSA), f"Public key {ID_RSA_PUB} exists but private key {ID_RSA} not found, delete {ID_RSA_PUB} and run again to regenerate pair"
    log(f"Found local keypair {ID_RSA}")
  elif os.path.exists(ID_RSA):
    assert os.path.exists(ID_RSA_PUB), f"Private key {ID_RSA} exists but public key {ID_RSA_PUB} not found, delete {ID_RSA} and run again to regenerate pair"
    log(f"Found local keypair {ID_RSA}")
  else:
    log(f"Generating keypair {ID_RSA}")
    with portalocker.Lock(ID_RSA+'.lock', timeout=5) as _:
      os.system(f"ssh-keygen -t rsa -f {ID_RSA} -N ''")
    os.system(f'rm {ID_RSA}.lock')

  return ID_RSA_PUB


def get_authorized_keys() -> str:
  """Appends local public key to NCLUSTER_AUTHORIZED_KEYS and returns in format key1;key2;key3;
  The result can be assigned back to NCLUSTER_AUTHORIZED_KEYS env var"""

  assert os.path.exists(ID_RSA_PUB), f"{ID_RSA_PUB} not found, make sure to run 'ncluster keys'"

  current_key = open(ID_RSA_PUB).read().strip()
  auth_keys = os.environ.get('NCLUSTER_AUTHORIZED_KEYS', '')
  return auth_keys+';'+current_key+';'


def get_public_key() -> str:
  """Returns public key, creating it if needed"""

  if not os.path.exists(ID_RSA_PUB):
    print(f"{ID_RSA_PUB} not found, running sure to run setup_local_ssh_keys()")
    setup_local_ssh_keys()
    assert os.path.exists(ID_RSA_PUB)

  return open(ID_RSA_PUB).read().strip()
