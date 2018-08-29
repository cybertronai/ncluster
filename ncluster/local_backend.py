# Local implementation of backend.py using separate tmux sessions for jobs

import os
import shlex
import subprocess
import sys
import time

from . import backend
from . import util

TASKDIR_ROOT = '/tmp/tasklogs'
LOGDIR_ROOT = os.environ['HOME']+'/ncluster/runs'  # use local instead of /tmp because /tmp gets wiped

# TODO: use separate session for each task, for parity with AWS job launcher


# todo: tmux session names are backwards from AWS job names (runname-jobname)
# TODO: add kwargs so that tmux backend can be drop-in replacement


# TODO: rename extra_kwargs to kwargs everywhere
class Task(backend.Task):
  """Local tasks interact with tmux session where session name is derived
  from job name, and window names are task ids."""

  def __init__(self, name, tmux_window, *, install_script='', job=None,
               **kwargs):
    self.tmux_window = tmux_window  # TODO: rename tmux_window to window?
    self.name = name
    self.install_script = install_script
    self.job = job
    self.kwargs = kwargs

    # local servers sometimes listen only on localhost (TensorBoard), and sometimes only on
    # externally assigned ip address from gethostbyname (Ray), use the localhost arbitrarily
    # https://github.com/ray-project/ray/issues/1677
    #    self.public_ip = socket.gethostbyname(socket.gethostname())
    self.public_ip = '127.0.0.1'
    self.ip = self.public_ip

    self.connect_instructions = 'tmux a -t ' + self.tmux_window

    # task current dir
    self.taskdir = f"{TASKDIR_ROOT}/{name}.{util.now_micros()}"
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
    if async:
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

  def get_logdir_root(self):
    return LOGDIR_ROOT

  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    """Uploads file to remote instance. If location not specified, dumps it
    into default directory."""

    self._log('uploading ' + local_fn)

    if remote_fn is None:
      remote_fn = os.path.basename(local_fn)
    if dont_overwrite and self.file_exists(remote_fn):
      self._log("Remote file %s exists, skipping" % (remote_fn,))
      return

    if os.path.isdir(local_fn):
      raise NotImplemented()

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

  def _wait_for_file(self, fn, max_wait_sec=600, check_interval=0.02):
    print("Waiting for file", fn)
    start_time = time.time()
    while True:
      if time.time() - start_time > max_wait_sec:
        assert False, "Timeout %s exceeded for %s" % (max_wait_sec, fn)
      if not self.file_exists(fn):
        time.sleep(check_interval)
        continue
      else:
        break

  def run2(self, cmd, async=False, ignore_errors=False):
    cmd_stdout_fn = f'{self._scratch}/{self._run_counter}.stdout'
    assert '|' not in cmd, "don't support piping (since we append piping here)"
    cmd = f'{cmd} | tee {cmd_stdout_fn}'
    self.run(cmd, async, ignore_errors)
    return self.file_read(cmd_stdout_fn)

  def _run_raw(self, cmd):
    """Runs command directly, skipping tmux interface"""
    os.system(cmd)


def make_task(name=None,
              run_name=None,
              **kwargs) -> Task:
  if name is None:
    name = f"{util.now_micros()}"

  # tmux can't use . for session names
  tmux_window = name.replace('.', '-') + ':0'
  tmux_session = tmux_window[:-2]
  util.log(f'killing session {tmux_session}')
  os.system(f'tmux kill-session -t {tmux_session}')
  os.system(f'tmux new-session -s {tmux_session} -n 0 -d')

  dummy_run = backend.Run(run_name)
  dummy_job = dummy_run.make_job()
  task = Task(name, job=dummy_job,
              tmux_window=tmux_window,  # propagate optional args
              **kwargs)
  dummy_job.tasks.append(task)
  return task


def make_job(name=None,
             num_tasks=0,
             run_name=None,
             **kwargs
             ) -> backend.Job:
  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"

  assert name.count('.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"
  tasks = [make_task(f"{i}.{name}") for i in range(num_tasks)]

  dummy_run = backend.Run(run_name)
  job = backend.Job(name, dummy_run, tasks, **kwargs)
  dummy_run.jobs.append(job)
  return job


def make_run(name) -> backend.Run:
  return backend.Run(name)
