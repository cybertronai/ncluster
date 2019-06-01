"""
Various helper utilities used internally by ncluster project, but may be potentially
useful outside of the cluster project.
"""

import os
import random
import string
import sys
import time
from collections import Iterable
import shlex

# starting value for now_micros (Aug 31, 2018)
# using this to make various timestamped names shorter
EPOCH_MICROS = 1535753974788163


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
      1. CTRL+\ breaks into pdb.
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


def is_set(name):
  """Helper method to check if given property is set"""
  val = os.environ.get(name, '0')
  assert val == '0' or val == '1', f"env var {name} has value {val}, expected 0 or 1"
  return val == '1'


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

def is_set(env_name) -> bool:
  if env_name not in os.environ:
    return False
  val = os.environ[env_name]
  return val != '' and val != '0' and val != 'false'
