# AWS implementation of backend.py

# todo: move EFS mounting into userdata for things to happen in parallel
# TODO: fix remote_fn must be absolute for uploading with check_with_existing
import os
import shlex
import sys
import threading
import time

from . import backend
from . import aws_util as u
from . import util
from . import aws_create_resources as create_lib

TMPDIR = '/tmp/ncluster'  # location for temp files on launching machine
LOGDIR_ROOT = '/ncluster/runs'


class Task(backend.Task):
  """AWS task is initialized with an AWS instance and handles initialization,
  creation of SSH session, shutdown"""

  def __init__(self, name, instance, *, install_script='', job=None,
               **extra_kwargs):

    self._can_run = False  # indicates that things needed for .run were created
    self.initialize_called = False

    self.name = name
    self.instance = instance
    self.install_script = install_script
    self.job = job
    self.extra_kwargs = extra_kwargs

    self.public_ip = u.get_public_ip(instance)
    self.ip = u.get_ip(instance)
    self._linux_type = 'ubuntu'

    # heuristic to tell if I'm using Amazon image name
    # default image has name like 'amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2'
    image_name = extra_kwargs.get('image_name', '').lower()
    if 'amzn' in image_name or 'amazon' in image_name:
      self._log('Detected Amazon Linux image')
      self._linux_type = 'amazon'
    self._run_counter = 0

    self._local_scratch = f"{TMPDIR}/{name}"
    self._remote_scratch = f"{TMPDIR}/{name}"

    os.system('mkdir -p ' + self._local_scratch)

    self._initialized_fn = f'{TMPDIR}/{self.name}.initialized'

    # _current_directory tracks current directory on task machine
    # used for uploading without specifying absolute path on target machine
    if self._linux_type == 'ubuntu':
      #      self._current_directory = '/home/ubuntu'
      self.ssh_username = 'ubuntu'  # default username on task machine
    elif self._linux_type == 'amazon':
      #      self._current_directory = '/home/ec2-user'
      self.ssh_username = 'ec2-user'

    self.ssh_client = u.ssh_to_task(self)
    self._setup_tmux()
    self._run_raw('mkdir -p ' + self._remote_scratch)
    self._mount_efs()

    if self._is_initialized_fn_present():
      self._log("reusing previous initialized state")
    else:
      self._log("running install script")

      # bin/bash needed to make self-executable or use UserData
      self.install_script = '#!/bin/bash\n' + self.install_script
      self.install_script += f'\necho ok > {self._initialized_fn}\n'
      self.file_write('install.sh', util.shell_add_echo(self.install_script))
      self.run('bash -e install.sh')  # fail on errors
      # TODO(y): propagate error messages printed on console to the user
      # right now had to log into tmux to see them
      assert self._is_initialized_fn_present()

    self.connect_instructions = f"""
ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no {self.ssh_username}@{self.public_ip}
tmux a
""".strip()
    self._log("Initialize complete")
    self._log(self.connect_instructions)

  # todo: replace with file_read
  def _is_initialized_fn_present(self):
    self._log("Checking for initialization status")
    try:
      return 'ok' in self.file_read(self._initialized_fn)
    except Exception:
      return False

  def get_logdir_root(self):
    return LOGDIR_ROOT

  def _setup_tmux(self):
    self._log("Setting up tmux")

    self._tmux_window = f"{self.name}-main:0".replace('.', '-')
    tmux_session_name = self._tmux_window[:-2]

    tmux_cmd = [f'tmux set-option -g history-limit 50000 \; ',
                f'set-option -g mouse on \; ',
                f'new-session -s {tmux_session_name} -n 0 -d']

    # hack to get around Amazon linux not having tmux
    if self._linux_type == 'amazon':
      self._run_raw('sudo yum install tmux -y')
      del tmux_cmd[1]  # Amazon tmux is really old, no mouse option

    self._run_raw(f'tmux kill-session -t {tmux_session_name}')
    self._run_raw(''.join(tmux_cmd))

    self._can_run = True

  def _mount_efs(self):
    self._log("Mounting EFS")
    region = u.get_region()
    efs_id = u.get_efs_dict()[u.get_prefix()]
    dns = f"{efs_id}.efs.{region}.amazonaws.com"
    self.run('sudo mkdir -p /ncluster')

    # ignore error on remount (efs already mounted)
    self.run(f"sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dns}:/ /ncluster",
             ignore_errors=True)
    self.run('sudo chmod 777 /ncluster')

    # Hack below may no longer be needed
    # # make sure chmod is successful, hack to fix occasional permission errors
    # while 'drwxrwxrwx' not in self.run_and_capture_output('ls -ld /ncluster'):
    #   print(f"chmod 777 /ncluster didn't take, retrying in {TIMEOUT_SEC}")
    #   time.sleep(TIMEOUT_SEC)
    #   self.run('sudo chmod 777 /ncluster')

  def join(self):
    while not self._is_initialized_fn_present():
      self._log(f"wait_until_ready: Not initialized, retrying in {u.RETRY_INTERVAL_SEC}")
      time.sleep(u.RETRY_INTERVAL_SEC)

  # TODO: also chmod the file so that 755 files remain 755
  def upload(self, local_fn, remote_fn=None, dont_overwrite=False):
    """Uploads file to remote instance. If location not specified, dumps it
    into default directory."""

    self._log('uploading ' + local_fn)
    sftp = self.ssh_client.open_sftp()

    if remote_fn is None:
      remote_fn = os.path.basename(local_fn)
    if dont_overwrite and self.file_exists(remote_fn):
      self._log("Remote file %s exists, skipping" % (remote_fn,))
      return

    if os.path.isdir(local_fn):
      u._put_dir(sftp, local_fn, remote_fn)
    else:
      assert os.path.isfile(local_fn), "%s is not a file" % (local_fn,)
      sftp.put(local_fn, remote_fn)

  def download(self, remote_fn, local_fn=None):
    self._log("downloading %s" % remote_fn)
    sftp = self.ssh_client.open_sftp()
    if local_fn is None:
      local_fn = os.path.basename(remote_fn)
      self._log("downloading %s to %s" % (remote_fn, local_fn))
    sftp.get(remote_fn, local_fn)

  def file_exists(self, remote_fn):
    stdout, stderr = self._run_raw('stat ' + remote_fn)
    return 'No such file' not in stdout

  def file_write(self, remote_fn, contents):
    tmp_fn = self._local_scratch + '/' + str(util.now_micros())
    open(tmp_fn, 'w').write(contents)
    self.upload(tmp_fn, remote_fn)

  def file_read(self, remote_fn):
    tmp_fn = self._local_scratch + '/' + str(util.now_micros())
    self.download(remote_fn, tmp_fn)
    return open(tmp_fn).read()

  def _run_raw(self, cmd):
    """Runs given cmd in the task using current SSH session, returns
    stdout/stderr as strings. Because it blocks until cmd is done, use it for
    short cmds. Silently ignores failing commands.

    This is a barebones method to be used during initialization that have
    minimal dependencies (no tmux)
    :param cmd: command to run
    :return: stdour and stderr
    """
    #    self._log("run_ssh: %s"%(cmd,))

    # TODO(y), transition to SSHClient and assert fail on bad error codes
    # https://stackoverflow.com/questions/3562403/how-can-you-get-the-ssh-return-code-using-paramiko
    stdin, stdout, stderr = self.ssh_client.exec_command(cmd, get_pty=True)
    stdout_str = stdout.read().decode()
    stderr_str = stderr.read().decode()
    # TODO: use shell return status to see if it failed
    if 'command not found' in stdout_str or 'command not found' in stderr_str:
      self._log(f"command ({cmd}) failed with ({stdout_str}), ({stderr_str})")
      assert False, "run_ssh command failed"
    return stdout_str, stderr_str

  def run2(self, cmd, async=False, ignore_errors=False):
    # TODO: maybe piping is ok, check
    assert '|' not in cmd, "don't support piping (since we append piping here)"

    ts = str(util.now_micros())
    cmd_stdout_fn = self._remote_scratch + '/' + str(self._run_counter) + '.' + ts + '.out'
    cmd = f'{cmd} | tee {cmd_stdout_fn}'
    self.run(cmd, async, ignore_errors)
    return self.file_read(cmd_stdout_fn)

  def run(self, cmd, async=False, ignore_errors=False,
          max_wait_sec=365 * 24 * 3600, check_interval=0.5):
    """Runs command in tmux session."""

    assert self._can_run, ".run command is not yet available"  # TODO: remove

    self._run_counter += 1

    cmd: str = cmd.strip()
    if cmd.startswith('#'):  # ignore empty/commented out lines
      return

    self._log("tmux> %s", cmd)

    # locking to wait for command to finish
    ts = str(util.now_micros())
    cmd_fn_out = self._remote_scratch + '/' + str(self._run_counter) + '.' + ts + '.out'

    cmd = util.shell_strip_comment(cmd)
    assert '&' not in cmd, f"cmd {cmd} contains &, that breaks things"

    # modify command to dump shell success status into file
    modified_cmd = '%s; echo $? > %s' % (cmd, cmd_fn_out)
    modified_cmd = shlex.quote(modified_cmd)
    tmux_cmd = f"tmux send-keys -t {self._tmux_window} {modified_cmd} Enter"
    self._run_raw(tmux_cmd)
    if async:
      return

    # wait until command finishes
    start_time = time.time()

    while True:
      if time.time() - start_time > max_wait_sec:
        assert False, "Timeout %s exceeded for %s" % (max_wait_sec, cmd)
      if not self.file_exists(cmd_fn_out):
        self._log("waiting for %s" % (cmd,))
        time.sleep(check_interval)
        continue

      contents = self.file_read(cmd_fn_out)
      # if empty wait a bit to allow for race condition
      if len(contents) == 0:
        time.sleep(check_interval)
        contents = self.file_read(cmd_fn_out)

      contents = contents.strip()
      if contents != '0':
        if not ignore_errors:
          assert False, "Command %s returned status %s" % (cmd, contents)
        else:
          self._log("Warning: command %s returned status %s" % (cmd, contents))
      break


# TODO: remove?
class Job(backend.Job):
  pass


def maybe_start_instance(instance):
  """Starts instance if it's stopped, no-op otherwise."""

  if not instance:
    return

  if instance.state['Name'] == 'stopped':
    instance.start()
    while True:
      print("Waiting forever for instances to start. TODO: replace with proper wait loop")
      time.sleep(10)


# TODO: call tasks without names "unnamed-123123"
def maybe_create_resources():
  """Use heuristics to decide to possibly create resources"""

  def should_create_resources():
    """Check if gateway, keypair, vpc exist."""
    prefix = u.get_prefix()
    if u.get_keypair_name() not in u.get_keypair_dict():
      print(f"Missing {u.get_keypair_name()} keypair, creating resources")
      return True
    vpcs = u.get_vpc_dict()
    if prefix not in vpcs:
      print(f"Missing {prefix} vpc, creating resources")
      return True
    vpc = vpcs[prefix]
    gateways = u.get_gateway_dict(vpc)
    if prefix not in gateways:
      print(f"Missing {prefix} gateway, creating resources")
      return True
    return False

  if should_create_resources():
    create_lib.create_resources()


def set_aws_environment():
  """Sets current zone/region environment variables."""
  current_zone = os.environ.get('AWS_ZONE', '')
  current_region = os.environ.get('AWS_DEFAULT_REGION', '')

  if current_region and current_zone:
    assert current_zone.startswith(current_region)
    assert u.get_session().region_name == current_region  # setting from ~/.aws

  # zone is set, set region from zone
  if current_zone and not current_region:
    current_region = current_zone[:-1]
    os.environ['AWS_DEFAULT_REGION'] = current_region

  # neither zone nor region not set, use default setting for region
  if not current_region:
    current_region = u.get_session().region_name
    os.environ['AWS_DEFAULT_REGION'] = current_region

  # zone not set, use first zone of the region
  if not current_zone:
    current_zone = current_region + 'a'
    os.environ['AWS_ZONE'] = current_zone

  util.log(f"Using account {u.get_account_number()}, zone {current_zone}")


def make_task(name: str = None,
              run_name: str = None,
              install_script: str = '',
              # image_name='Deep Learning AMI (Ubuntu) Version 12.0',
              image_name: str = '',
              instance_type: str = 't3.micro') -> Task:
  maybe_create_resources()
  set_aws_environment()

  if name is None:
    name = f"{u.get_prefix()}-{util.now_micros()}"
  instance = u.lookup_instance(name)  # todo: also add kwargs
  maybe_start_instance(instance)

  if not image_name:
    image_name = os.environ.get('NCLUSTER_IMAGE',
                                'amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')
    util.log("Using image ", image_name)
  image = u.lookup_image(image_name)
  keypair = u.get_keypair()
  security_group = u.get_security_group()
  subnet = u.get_subnet()
  ec2 = u.get_ec2_resource()

  # create the instance if not present
  if not instance:
    util.log(f"Allocating {instance_type} for task {name}")
    args = {'ImageId': image.id,
            'InstanceType': instance_type,
            'MinCount': 1,
            'MaxCount': 1,
            'KeyName': keypair.name}

    args['TagSpecifications'] = [{
      'ResourceType': 'instance',
      'Tags': [{
        'Key': 'Name',
        'Value': name
      }]
    }]
    args['NetworkInterfaces'] = [{'SubnetId': subnet.id,
                                  'DeviceIndex': 0,
                                  'AssociatePublicIpAddress': True,
                                  'Groups': [security_group.id]}]
    placement_specs = {'AvailabilityZone': u.get_zone()}

    # if placement_group: placement_specs['GroupName'] = placement_group
    args['Placement'] = placement_specs
    args['Monitoring'] = {'Enabled': True}

    try:
      instances = ec2.create_instances(**args)
    except Exception as e:
      print(f"Instance creation for {name} failed with ({e})")
      print("You can change availability zone using export AWS_ZONE=...")
      sys.exit()

    assert instances, f"ec2.create_instances returned {instances}"
    util.log(f"Allocated {len(instances)} instances")
    instance = instances[0]

  dummy_run = backend.Run(run_name)
  dummy_job = dummy_run.make_job()
  task = Task(name, instance,  # propagate optional args
              install_script=install_script,
              image_name=image_name,
              instance_type=instance_type)
  dummy_job.tasks.append(task)
  return task


def make_job(name, num_tasks=0, run_name=None, **kwargs) -> Job:
  assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"

  assert name.count('.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"

  # make tasks in parallel
  exceptions = []
  tasks = [backend.Task()] * num_tasks

  def make_task_fn(i: int):
    try:
      tasks[i] = make_task(f"{i}.{name}", **kwargs)
    except Exception as e:
      exceptions.append(e)

  threads = [threading.Thread(name=f'make_task_{i}',
                              target=make_task_fn, args=[i])
             for i in range(num_tasks)]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()
  if exceptions:
    raise exceptions[0]

  dummy_run = backend.Run(run_name)
  job = Job(name, dummy_run, tasks, **kwargs)
  dummy_run.jobs.append(job)
  return job

# def make_run(name, **kwargs):
#  return Run(name, **kwargs)
