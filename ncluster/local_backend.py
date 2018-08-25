# Local implementation of backend.py using separate tmux sessions for jobs

import datetime
import os
import subprocess
import shlex
import sys
import socket
import time

from . import backend
from . import util

TASKDIR_PREFIX = '/tmp/tasklogs'


# TODO: use separate session for each task, for parity with AWS job launcher


# todo: tmux session names are backwards from AWS job names (runname-jobname)
# TODO: add kwargs so that tmux backend can be drop-in replacement
def make_run(name, install_script='', **kwargs):
  if kwargs:
    print("Warning, unused kwargs", kwargs)
  return Run(name, install_script)


class Run(backend.Run):
  def __init__(self, name, install_script=''):
    self.name = name
    self.install_script = install_script
    self.jobs = []
    self.logdir = f'{backend.LOGDIR_PREFIX}/{self.name}'

  # TODO: rename job_name to role_name
  def make_job(self, job_name, num_tasks=1, install_script='', **kwargs):
    assert num_tasks >= 0

    if kwargs:
      print("Warning, unused kwargs", kwargs)

    # TODO, remove mandatory delete and make separate method for killing?
    tmux_name = self.name + '-' + job_name  # tmux can't use . in name
    os.system('tmux kill-session -t ' + tmux_name)
    tmux_windows = []
    self.log("Creating %s with %s" % (tmux_name, num_tasks))
    if num_tasks > 0:
      os.system('tmux new-session -s %s -n %d -d' % (tmux_name, 0))
      tmux_windows.append(tmux_name + ":" + str(0))
    for task_id in range(1, num_tasks):
      os.system("tmux new-window -t {} -n {}".format(tmux_name, task_id))
      tmux_windows.append(tmux_name + ":" + str(task_id))

    if not install_script:
      install_script = self.install_script
    job = Job(self, job_name, tmux_windows, install_script=install_script)
    self.jobs.append(job)
    return job

  def setup_logdir(self):
    if os.path.exists(self.logdir):
      datestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
      new_logdir = f'{backend.LOGDIR_PREFIX}/{self.name}.{datestr}'
      self.log(f'Warning, logdir {self.logdir} exists, deduping to {new_logdir}')
      self.logdir = new_logdir
    os.makedirs(self.logdir)


class Job(backend.Job):
  def __init__(self, run, name, tmux_windows, install_script=''):
    self._run = run
    self.name = name
    self.tasks = []
    for task_id, tmux_window in enumerate(tmux_windows):
      self.tasks.append(Task(tmux_window, self, install_script=install_script))


# TODO: rename extra_kwargs to kwargs everywhere
class Task(backend.Task):
  """Local tasks interact with tmux session where session name is derived
  from job name, and window names are task ids."""

  def __init__(self, name, tmux_window, *, install_script='', job=None,
               **extra_kwargs):
    self.tmux_window = tmux_window  # TODO: rename tmux_window to window?
    self.name = name
    self.install_script = install_script
    self.job = job
    self.extra_kwargs = extra_kwargs

    self.public_ip = socket.gethostbyname(socket.gethostname())
    self.ip = self.public_ip

    self.connect_instructions = 'tmux a -t ' + self.tmux_window

    # task current dir
    self.taskdir = f"{TASKDIR_PREFIX}/{name}.{util.now_micros()}"
    self._log("Creating taskdir %s", self.taskdir)
    self._scratch = self.taskdir + '/scratch'

    self._run_raw('mkdir -p ' + self.taskdir)
    self._run_raw('rm -Rf ' + self._scratch)
    self._run_raw('mkdir -p ' + self._scratch)
    self._run_counter = 0

    self.run('cd ' + self.taskdir)
    self.install_script = install_script
    for line in install_script.split('\n'):
      self.run(line)

  def _wait_for_file(self, fn, max_wait_sec=600, check_interval=0.02):
    start_time = time.time()
    while True:
      if time.time() - start_time > max_wait_sec:
        assert False, "Timeout %s exceeded for %s" % (max_wait_sec, fn)
      if not self.file_exists(fn):
        time.sleep(check_interval)
        continue
      else:
        break

  def run(self, cmd, async=False, ignore_errors=False, **kwargs):
    self._run_counter += 1
    cmd = cmd.strip()
    if not cmd or cmd.startswith('#'):  # ignore empty/commented out lines
      return
    self._log("tmux> %s", cmd)

    cmd_in_fn = '%s/%d.in' % (self._scratch, self._run_counter)
    cmd_out_fn = '%s/%d.out' % (self._scratch, self._run_counter)
    assert not os.path.exists(cmd_out_fn)

    cmd = util.shell_strip_comment(cmd)
    assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    open(cmd_in_fn, 'w').write(cmd + '\n')
    modified_cmd = '%s ; echo $? > %s' % (cmd, cmd_out_fn)
    modified_cmd = shlex.quote(modified_cmd)

    tmux_cmd = f'tmux send-keys -t {self.tmux_window} {modified_cmd} Enter'
    self._run_raw(tmux_cmd)
    if not async:
      return

    # TODO: dedup this with file waiting logic in aws_backend
    self._wait_for_file(cmd_out_fn)
    contents = open(cmd_out_fn).read()

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(0.1)
    contents = open(cmd_out_fn).read().strip()

    if contents != '0':
      if not ignore_errors:
        assert False, "Command %s returned status %s" % (cmd, contents)
      else:
        self._log("Warning: command %s returned status %s" % (cmd, contents))

  def _run_and_capture_output(self, cmd, async=False, ignore_errors=False):
    cmd_stdout_fn = f'{self._scratch}/{self._run_counter}.stdout'
    assert '|' not in cmd, "don't support piping (since we append piping here)"
    cmd = f'{cmd} | tee {cmd_stdout_fn}'
    self.run(cmd, async, ignore_errors)
    return self.file_read(cmd_stdout_fn)

  def _run_raw(self, cmd):
    """Runs command directly, skipping tmux interface"""
    #    self.log(cmd)
    os.system(cmd)

  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    #    self.log("uploading %s to %s"%(source_fn, target_fn))
    local_fn_full = os.path.abspath(local_fn)
    self.run("cp -R %s %s" % (local_fn_full, remote_fn))

  def download(self, source_fn, target_fn='.'):
    raise NotImplementedError()
    # self.log("downloading %s to %s"%(source_fn, target_fn))
    # source_fn_full = os.path.abspath(source_fn)
    # os.system("cp %s %s" %(source_fn_full, target_fn))

  def file_exists(self, remote_fn):
    return os.path.exists(remote_fn)

  def file_read(self, remote_fn):
    return open(remote_fn).read()

  def file_write(self, remote_fn, contents):
    def make_temp_fn():
      """Returns temporary filename for this task."""
      return self._scratch + '/file_write.' + str(util.now_micros())

    tmp_fn = make_temp_fn()
    open(tmp_fn, 'w').write(contents)
    self.upload(tmp_fn, remote_fn)

  def _stream_file(self, fn):
    if not fn.startswith('/'):
      fn = self.taskdir + '/' + fn

    if not os.path.exists(fn):
      os.system('mkdir -p ' + os.path.dirname(fn))
      os.system('touch ' + fn)

    p = subprocess.Popen(['tail', '-f', fn], stdout=subprocess.PIPE)

    for line in iter(p.stdout.readline, ''):
      sys.stdout.write(line.decode('ascii', errors='ignore'))


def make_task(name=None,
              install_script='',
              **kwargs) -> Task:
  if name is None:
    name = f"{util.now_micros()}"
    
  assert '.' not in name, "tmux can't use . in session name"
  tmux_window = name + ':0'
  tmux_session = tmux_window[:-2]
  util.log(f'killing session {tmux_session}')
  os.system(f'tmux kill-session -t {tmux_session}')
  os.system(f'tmux new-session -s {tmux_session} -n 0 -d')

  task = Task(name, tmux_window,  # propagate optional args
              install_script=install_script,
              **kwargs)
  return task
