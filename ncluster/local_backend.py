# Local implementation of backend.py using separate tmux sessions for jobs
import glob
import os
import shlex
import socket
import sys
import time

from . import backend
from . import util

TASKDIR_ROOT = '/tmp/ncluster/task'
SCRATCH_ROOT = '/tmp/ncluster/scratch'
LOGDIR_ROOT = os.environ[
                'HOME'] + '/ncluster/runs'  # use ~ instead of /tmp because /tmp gets wiped


# TODO: use separate session for each task, for parity with AWS job launcher


# todo: tmux session names are backwards from AWS job names (runname-jobname)
# TODO: add kwargs so that tmux backend can be drop-in replacement


# TODO: rename extra_kwargs to kwargs everywhere
class Task(backend.Task):
  """Local tasks interact with tmux session where session name is derived
  from job name, and window names are task ids."""

  def __init__(self, name, tmux_window, *, install_script='', job=None,
               **kwargs):
    self._can_run = False
    self.tmux_window = tmux_window
    self.name = name
    self.install_script = install_script
    self.job = job
    self.kwargs = kwargs

    # local servers sometimes listen only on localhost (TensorBoard), and sometimes only on
    # externally assigned ip address from gethostbyname (Ray), must choose one, so use the localhost for TB compatibility
    # https://github.com/ray-project/ray/issues/1677
    self.public_ip = socket.gethostbyname(socket.gethostname())
    #  self.public_ip = '127.0.0.1'
    self.ip = self.public_ip

    self.connect_instructions = 'tmux a -t ' + self.tmux_window

    # task current dir
    print('name is', name)
    tmpdir = f"{util.reverse_taskname(name)}.{os.getpid()}.{util.now_micros()}"
    self.taskdir = f"{TASKDIR_ROOT}/{tmpdir}"
    launch_id = util.random_id()
    self.local_scratch = f"{SCRATCH_ROOT}/{tmpdir}-{launch_id}"
    self.remote_scratch = f"{SCRATCH_ROOT}/{tmpdir}-{launch_id}"

    self.log(f"Creating taskdir {self.taskdir}")
    self._run_raw('mkdir -p ' + self.taskdir)

    self.log(f"Creating scratch {self.local_scratch}")
    self._run_raw('rm -Rf ' + self.local_scratch)
    self._run_raw('mkdir -p ' + self.local_scratch)
    self._run_raw('mkdir -p ' + self.remote_scratch)
    self.run_counter = 0

    self._cwd = self.taskdir
    self._can_run = True
    self.run('cd ' + self.taskdir)

    print("Running install script " + install_script)
    self.install_script = install_script
    for line in install_script.split('\n'):
      self.run(line)

  def run(self, cmd, non_blocking=False, ignore_errors=False, **kwargs) -> int:
    if not self._can_run:
      assert False, "Using .run before initialization finished"
    if '\n' in cmd:
      cmds = cmd.split('\n')
      self.log(
        f"Running {len(cmds)} commands at once, returning status of last")
      status = -1
      for subcmd in cmds:
        status = self.run(subcmd)
      return status

    cmd = cmd.strip()
    if not cmd or cmd.startswith('#'):  # ignore empty/commented out lines
      return -1
    self.run_counter += 1
    self.log("tmux> %s", cmd)

    cmd_fn = f'{self.local_scratch}/{self.run_counter}.cmd'
    status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
    assert not os.path.exists(status_fn)

    cmd = util.shell_strip_comment(cmd)
    # assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    self.file_write(cmd_fn, cmd + '\n')
    modified_cmd = f'{cmd} ; echo $? > {status_fn}'
    modified_cmd = shlex.quote(modified_cmd)

    tmux_cmd = f'tmux send-keys -t {self.tmux_window} {modified_cmd} Enter'
    self._run_raw(tmux_cmd)
    if non_blocking:
      return 0

    if not self.wait_for_file(status_fn, max_wait_sec=60):
      self.log(f"Retrying waiting for {status_fn}")
    while not self.file_exists(status_fn):
      self.log(f"Still waiting for {cmd}")
      self.wait_for_file(status_fn, max_wait_sec=60)
    contents = self.file_read(status_fn)

    # if empty wait a bit to allow for race condition
    if len(contents) == 0:
      time.sleep(0.01)
    status = int(open(status_fn).read().strip())

    if status != 0:
      if not ignore_errors:
        raise RuntimeError(f"Command {cmd} returned status {status}")
      else:
        self.log(f"Warning: command {cmd} returned status {status}")

    return status

  def _run_raw(self, cmd):
    """Runs command directly, skipping tmux interface"""
    os.system(cmd)

  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    """Uploads file to remote instance. If location not specified, dumps it
    into default directory. Creates missing directories in path name."""

    # support wildcard through glob
    if '*' in local_fn:
      for local_subfn in glob.glob(local_fn):
        self.upload(local_subfn)
      return

    if remote_fn is None:
      remote_fn = os.path.basename(local_fn)
    self.log('uploading ' + local_fn + ' to ' + remote_fn)

    if dont_overwrite and self.file_exists(remote_fn):
      self.log("Remote file %s exists, skipping" % (remote_fn,))
      return

    if not remote_fn.startswith('/'):
      remote_fn = self.taskdir + '/' + remote_fn

    local_fn = os.path.abspath(local_fn)
    self._run_raw("cp -R %s %s" % (local_fn, remote_fn))

  def download(self, remote_fn, local_fn='.'):
    if local_fn == '.':
      local_fn = self._cwd
    #    self.log("downloading %s to %s" % (remote_fn, local_fn))
    if not remote_fn.startswith('/'):
      remote_fn = self._cwd + '/' + remote_fn
    if self.file_exists(remote_fn):
      os.system(f'cp {remote_fn} {local_fn}')
    else:
      raise RuntimeError(f"No such file {remote_fn}")

  def file_exists(self, remote_fn):
    return os.path.exists(remote_fn)

  def file_read(self, remote_fn):
    tmp_fn = self.local_scratch + '/' + str(util.now_micros())
    self.download(remote_fn, tmp_fn)
    return open(tmp_fn).read()

  def file_write(self, remote_fn, contents):
    def make_temp_fn():
      """Returns temporary filename for this task."""
      return self.local_scratch + '/file_write.' + str(util.now_micros())

    tmp_fn = make_temp_fn()
    open(tmp_fn, 'w').write(contents)
    self.upload(tmp_fn, remote_fn)

  # don't include file streaming for now
  # the issue is that file streaming by default turns on 4K buffering, which makes
  # streaming a lot less useful. Similar buffering is turned on for piping commands
  # https://unix.stackexchange.com/questions/25372/turn-off-buffering-in-pipe
  # def file_stream(self, fn: str) -> None:
  #   #    if not fn.startswith('/'):
  #   #      fn = self.taskdir + '/' + fn
  #
  #   if not os.path.exists(fn):
  #     os.system('mkdir -p ' + os.path.dirname(os.path.abspath(fn)))
  #     os.system('touch ' + fn)
  #
  #   p = subprocess.Popen(['tail', '-f', fn], stdout=subprocess.PIPE)
  #
  #   for line in iter(p.stdout.readline, ''):
  #     sys.stdout.write(line.decode('ascii', errors='ignore'))


def make_task(name='',
              run_name='',
              **kwargs) -> Task:
  if not name:
    script_id = util.alphanumeric_hash(sys.argv[0])
    name = f"unnamedlocaltask-{script_id}"

  if not run_name:
    run_name = f'unnamedrun-{name}'

  # tmux can't use . for session names
  tmux_window = name.replace('.', '=') + ':0'
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
             run_=None,
             install_script='',
             **kwargs
             ) -> backend.Job:
  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"

  assert name.count(
    '.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"
  tasks = [make_task(f"{i}.{name}",
                     run_name=run_name,
                     install_script=install_script,
                     **kwargs
                     ) for i in range(num_tasks)]

  if run_ is None:
    run_ = backend.Run(run_name)

  job = backend.Job(name=name, run_=run_, tasks=tasks, **kwargs)
  run_.jobs.append(job)
  return job


def make_run(name) -> backend.Run:
  return backend.Run(name)
