import time
from collections import Iterable
import shlex


def is_iterable(k):
  return isinstance(k, Iterable)


def now_micros() -> int:
  """Return current micros since epoch as integer."""
  return int(time.time()*1e6)


def log_error(*args, **kwargs):
  print(f"Error encountered {args} {kwargs}")


def log(*args, **kwargs):
  print(f"{args} {kwargs}")


def install_pdb_handler():
  """Make CTRL+\ break into gdb."""
  
  import signal
  import pdb

  def handler(_signum, _frame):
    pdb.set_trace()
  signal.signal(signal.SIGQUIT, handler)


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
    new_script += cmd+"\n"
  return new_script


def shell_strip_comment(cmd):
  """ hi # testing => hi"""
  if '#' in cmd:
    return cmd.split('#', 1)[0]
  else:
    return cmd
