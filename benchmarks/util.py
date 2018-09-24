import os
import subprocess
import sys


class FileLogger:
  """Helper class to log to file (possibly mirroring to stderr)
     logger = FileLogger('somefile.txt')
     logger = FileLogger('somefile.txt', mirror=True)
     logger('somemessage')
     logger('somemessage: %s %.2f', 'value', 2.5)
  """

  def __init__(self, fn, mirror=True):
    self.fn = fn
    self.f = open(fn, 'w')
    self.mirror = mirror
    print(f"Creating FileLogger on {os.path.abspath(fn)}")

  def __call__(self, s='', *args):
    """Either ('asdf %f', 5) or (val1, val2, val3, ...)"""
    if (isinstance(s, str) or isinstance(s, bytes)) and '%' in s:
      formatted_s = s % args
    else:
      toks = [s] + list(args)
      formatted_s = ', '.join(str(s) for s in toks)

    self.f.write(formatted_s + '\n')
    self.f.flush()
    if self.mirror:
      # use manual flushing because "|" makes output 4k buffered instead of
      # line-buffered
      sys.stdout.write(formatted_s+'\n')
      sys.stdout.flush()

  def __del__(self):
    self.f.close()


def ossystem(cmd):
  """Like os.system, but returns output of command as string."""
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT)
  (stdout, stderr) = p.communicate()
  return stdout.decode('ascii')
