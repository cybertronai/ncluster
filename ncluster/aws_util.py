"""Methods used in aws_backend, but also useful for standalone prototyping in Jupyter."""

import os
import re
import subprocess
import sys
import time
from collections import OrderedDict

import botocore.exceptions
import paramiko
from operator import itemgetter

from typing import Iterable, List, Dict, Optional

from boto3_type_annotations.ec2 import SecurityGroup, Vpc, Subnet, InternetGateway, PlacementGroup, Image, \
  Instance, KeyPairInfo
from boto3.resources.collection import ResourceCollection

from boto3_type_annotations.ec2 import ServiceResource as EC2_ServiceResource
from boto3_type_annotations.ec2 import Client as EC2_Client
from boto3_type_annotations.sts import Client as STS_Client

import boto3

from . import util

from .util import VALID_REGIONS

EMPTY_NAME = "noname"  # name to use when name attribute is missing on AWS
RETRY_INTERVAL_SEC = 1  # how long to wait before retries
RETRY_TIMEOUT_SEC = 60  # how long to wait before retrying fails
DEFAULT_PREFIX = 'ncluster'
PRIVATE_KEY_LOCATION = os.environ['HOME'] + '/.ncluster'
DUPLICATE_CHECKING = False

# allow referring to local methods as u.<method>, for easier moving of code between this module and others
u = sys.modules[__name__]


# Can't annotate boto3 return types because they are missing stubs
# https://github.com/boto/boto3/issues/1055
# https://stackoverflow.com/questions/52087307/adding-type-hinting-to-functions-that-return-boto3-objects

def get_vpc() -> Vpc:
  """
  Returns current VPC (ec2.Vpc object)
  https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#vpc
  """

  return get_vpc_dict()[get_prefix()]


def get_security_group() -> SecurityGroup:
  """
  Returns current security group, ec2.SecurityGroup object
  https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#securitygroup
"""
  return get_security_group_dict()[get_security_group_name()]


def get_security_group_nd() -> SecurityGroup:
  """Gets security group associated with non-default VPC"""
  return get_security_group_dict()[get_security_group_nd_name()]


def get_subnet() -> Subnet:
  zone = get_zone()
  assert zone, f"Subnet is only defined for zone, current zone is '{zone}', use $NCLUSTER_ZONE to define"
  subnet_dict = get_subnet_dict()
  assert zone in subnet_dict, f"zone '{zone}' is not in subnet_dict, available zones are {subnet_dict.keys()}"
  return subnet_dict[zone]


def get_vpc_dict() -> Dict[str, Vpc]:
  """Returns dictionary of named VPCs {name: vpc}

  Assert fails if there's more than one VPC with same name."""

  client = get_ec2_client()
  response = client.describe_vpcs()
  assert is_good_response(response)

  result = OrderedDict()
  ec2 = get_ec2_resource()
  for vpc_response in response['Vpcs']:
    key = get_name(vpc_response.get('Tags', []))
    if not key or key == EMPTY_NAME:  # skip VPC's that don't have a name assigned
      continue

    if key in result:
      util.log(f"Warning: Duplicate VPC group {key} in {response}")
      if DUPLICATE_CHECKING:
        assert False
    result[key] = ec2.Vpc(vpc_response['VpcId'])

  return result


def get_default_vpc() -> Vpc:
  """
  Return default VPC or none if not present

  """
  ec2 = get_ec2_resource()
  for vpc in ec2.vpcs.all():
    if vpc.is_default:
      return vpc


def get_subnet_dict() -> Dict[str, Subnet]:
  """Returns dictionary of "availability zone" -> subnet for current VPC."""
  subnet_dict = {}
  vpc = get_default_vpc()
  for subnet in vpc.subnets.all():
    zone = subnet.availability_zone
    assert zone not in subnet_dict, "More than one subnet in %s, why?" % (zone,)
    subnet_dict[zone] = subnet
  return subnet_dict


def get_gateway_dict(vpc) -> Dict[str, InternetGateway]:
  """Returns dictionary of named gateways for given VPC {name: gateway}"""
  return {get_name(gateway): gateway for
          gateway in vpc.internet_gateways.all()}


def get_efs_dict() -> Dict[str, str]:
  """Returns dictionary of {efs_name: efs_id}"""
  # there's no EC2 resource for EFS objects, so return EFS_ID instead
  # https://stackoverflow.com/questions/47870342/no-ec2-resource-for-efs-objects

  efs_client = get_efs_client()
  response = call_with_retries(efs_client.describe_file_systems,
                               'efs_client.describe_file_systems')
  assert is_good_response(response)
  result = OrderedDict()
  for efs_response in response['FileSystems']:
    fs_id = efs_response['FileSystemId']

    tag_response = call_with_retries(efs_client.describe_tags,
                                     "efs_client.describe_tags",
                                     FileSystemId=fs_id, retry_interval_sec=2)
    assert is_good_response(tag_response)
    key = get_name(tag_response['Tags'])
    if not key or key == EMPTY_NAME:  # skip EFS's without a name
      continue
    assert key not in result
    result[key] = fs_id

  return result


def get_placement_group_dict() -> Dict[str, PlacementGroup]:
  """Returns dictionary of {placement_group_name: (state, strategy)}"""

  client = get_ec2_client()
  response = client.describe_placement_groups()
  assert is_good_response(response)

  result = OrderedDict()
  ec2 = get_ec2_resource()
  for placement_group_response in response['PlacementGroups']:
    key = placement_group_response['GroupName']
    if key in result:
      util.log(f"Warning: Duplicate placement_group group {key}")
      if DUPLICATE_CHECKING:
        assert False
    result[key] = ec2.PlacementGroup(key)
  return result


def get_security_group_dict() -> Dict[str, SecurityGroup]:
  """Returns dictionary of named security groups {name: securitygroup}."""

  client = get_ec2_client()
  response = client.describe_security_groups()
  assert is_good_response(response)

  result = OrderedDict()
  ec2 = get_ec2_resource()
  for security_group_response in response['SecurityGroups']:
    key = get_name(security_group_response.get('Tags', []))
    if not key or key == EMPTY_NAME:
      continue  # ignore unnamed security groups
    #    key = security_group_response['GroupName']
    if key in result:
      util.log(f"Warning: Duplicate security group {key}")
      if DUPLICATE_CHECKING:
        assert key not in result, ("Duplicate security group " + key)
    result[key] = ec2.SecurityGroup(security_group_response['GroupId'])

  return result


def get_keypair_dict() -> Dict[str, KeyPairInfo]:
  """Returns dictionary of {keypairname: keypair}"""

  client = get_ec2_client()
  response = client.describe_key_pairs()
  assert is_good_response(response)

  result = {}
  ec2 = get_ec2_resource()
  for keypair in response['KeyPairs']:
    keypair_name = keypair.get('KeyName', '')
    if keypair_name in result:
      util.log(f"Warning: Duplicate key {keypair_name}")
    if DUPLICATE_CHECKING:
      assert keypair_name not in result, "Duplicate key " + keypair_name
    result[keypair_name] = ec2.KeyPair(keypair_name)
  return result


def get_prefix() -> str:
  """Global prefix to identify ncluster created resources name used to identify ncluster created resources,
  (name of EFS, VPC, keypair prefixes), can be changed through $NCLUSTER_PREFIX for debugging purposes. """

  name = os.environ.get('NCLUSTER_PREFIX', DEFAULT_PREFIX)
  if name != DEFAULT_PREFIX:
    validate_prefix(name)
  return name


def get_account_number() -> str:
  start_time = time.time()
  success = False
  account_number = "Unknown"
  retry_times = 3
  for i in range(retry_times):
    if time.time() - start_time - RETRY_INTERVAL_SEC > RETRY_TIMEOUT_SEC:
      assert False, f"Timeout {RETRY_TIMEOUT_SEC} exceeded querying account number {retry_times} times"
    try:
      sts_client: STS_Client = boto3.client('sts')
      account_number = str(sts_client.get_caller_identity()['Account'])
      success = True
    except Exception as e:
      util.log(f'Exception in get_account_number {e}, retrying')
      if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        util.log(
          'AWS_SECRET_ACCESS_KEY not in env vars, configure your AWS credentials."')
      if 'AWS_ACCESS_KEY_ID' not in os.environ:
        util.log(
          'AWS_ACCESS_KEY_ID not in env vars, configure your AWS credentials."')
      time.sleep(RETRY_INTERVAL_SEC)
  public_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
  if not success:
    assert False, f"Could access account, make sure you have correct credentials for region {get_region()} and key {public_key}"
  return account_number


def get_account_name() -> str:
  iam_client = boto3.client('iam')
  response = iam_client.list_account_aliases()
  aliases = response.get('AccountAliases', [])
  if aliases:
    return aliases[0]
  else:
    return ''


def get_iam_username() -> str:
  iam_client = boto3.client('iam')
  return iam_client.get_user()['User']['UserName']


def get_region() -> str:
  return get_session().region_name


def set_region(region: str) -> None:
  assert region in VALID_REGIONS, f"region '{region}' is not valid, valid regions are [{','.join(VALID_REGIONS)}]"
  util.set_env('AWS_DEFAULT_REGION', region)


def get_zone() -> str:
  """Returns current zone, or empty string if it's unset."""
  return util.get_env('NCLUSTER_ZONE')


def set_zone(zone: str) -> None:
  current_region = u.get_region()
  assert zone.startswith(current_region), f'Current zone "{zone}" ($NCLUSTER_ZONE) is not in current region "{current_region} ($AWS_DEFAULT_REGION), change either $NCLUSTER_ZONE or AWS_DEFAULT_REGION'
  util.set_env('NCLUSTER_ZONE', zone)


def get_zones() -> List[str]:
  client = get_ec2_client()
  response = client.describe_availability_zones()
  assert is_good_response(response)
  zones = []
  for avail_response in response['AvailabilityZones']:
    messages = avail_response['Messages']
    zone = avail_response['ZoneName']
    state = avail_response['State']
    assert not messages, f"zone {zone} is broken? Has messages {messages}"
    assert state == 'available', f"zone {zone} is broken? Has state {state}"
    zones.append(zone)
  return zones


def get_session():
  # in future can add aws profile support with Session(profile_name=...)
  return boto3.Session()


################################################################################
# keypairs
################################################################################
# For naming conventions, see
# https://docs.google.com/document/d/14-zpee6HMRYtEfQ_H_UN9V92bBQOt0pGuRKcEJsxLEA/edit#heading=h.45ok0839c0a

def get_keypair_name() -> str:
  """Returns current keypair name."""

  username = get_username()
  assert '-' not in username, "username must not contain -, change $USER"
  validate_aws_name(username)
  assert len(username) < 30  # to avoid exceeding AWS 127 char limit
  return get_prefix() + '-' + username


def get_keypair() -> KeyPairInfo:
  """Returns current keypair (ec2.KeyPairInfo)

  https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#keypairinfo
  """

  return get_keypair_dict()[get_keypair_name()]


def get_keypair_fn() -> str:
  """Location of .pem file for current keypair"""

  keypair_name = get_keypair_name()
  account = get_account_number()
  region = get_region()
  fn = f'{PRIVATE_KEY_LOCATION}/{keypair_name}-{account}-{region}.pem'
  return fn


def get_vpc_name() -> str:
  return get_prefix()


def get_security_group_name() -> str:
  """Security group for default VPC"""
  return get_prefix()


def get_security_group_nd_name() -> str:
  """Security group for non-default VPC"""
  return get_prefix() + '_nd'


def get_gateway_name() -> str:
  return get_prefix()


def get_route_table_name() -> str:
  return get_prefix()


def get_efs_name() -> str:
  return get_prefix()


def get_username() -> str:
  assert 'USER' in os.environ, "why isn't USER defined?"
  return os.environ['USER']


def lookup_image(wildcard) -> Image:
  """Returns unique ec2.Image whose name matches wildcard
  lookup_ami('pytorch*').name => ami-29fa
  
  https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#image

  Assert fails if multiple images match or no images match.
  """

  ec2 = get_ec2_resource()
  filter_ = {'Name': 'name', 'Values': [wildcard]}

  images = list(ec2.images.filter(Filters=[filter_]))

  # Note, can add filtering by Owners as follows
  #  images = list(ec2.images.filter_(Filters = [filter_], Owners=['self', 'amazon']))

  assert len(images) <= 1, "Multiple images match " + str(wildcard)
  assert len(images) > 0, f"No images match {str(wildcard)} in {get_region()}, check name and image permissions"
  return images[0]


def is_amazon_ami(image_name):
  """Heuristic to tell if given image name is Amazon Linux image."""
  image_name = image_name.lower()
  return 'amzn' in image_name or 'amazon' in image_name or image_name.startswith('dlami23-efa') or image_name.startswith('basic-efa')


def get_aws_username(instance) -> str:
  image_name = instance.image.name.lower()
  if util.get_env('NCLUSTER_SSH_USERNAME'):
    return util.get_env('NCLUSTER_SSH_USERNAME')

  if is_amazon_ami(image_name):
    print("Auto-detected Amazon Linux, using ec2-user ssh name")
    return 'ec2-user'
  else:
    return 'ubuntu'


def lookup_instance_exact(name: str, instance_type: str = '', image_name: str = '',
                          states: tuple = ('running', 'stopped', 'initializing'),
                          limit_to_current_user=False) -> Optional[Instance]:
  """Looks up AWS instance for given instance name using exact matching, like
   simple.worker. If no instance found in current AWS environment, returns None. """

  ec2 = get_ec2_resource()

  # https://github.com/boto/boto3/issues/2000
  # noinspection PyTypeChecker
  instances: ResourceCollection = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': states}])

  prefix = get_prefix()
  username = get_username()

  # look for an existing instance matching job, ignore instances launched
  # by different user or under different resource name
  result = []
  for i in instances.all():
    instance_name = get_name(i)
    if instance_name != name:
      continue

    if limit_to_current_user and i.key_name != get_keypair_name():
      continue

    seen_prefix, seen_username = parse_key_name(i.key_name)
    if prefix != seen_prefix:
      print(f"Found {name} launched under {seen_prefix}, ignoring because current ncluster prefix is {prefix}")
      continue
    if username != seen_username:
      print(f"Found {name} launched by {seen_username}, ignoring because current user is {username}")
      continue

    if instance_type:
      assert i.instance_type == instance_type, f"Found existing instance for job {name} but different instance type ({i.instance_type}) than requested ({instance_type}), terminate {name} first or use new task name."

    if image_name:
      assert i.image.name == image_name, f"Found existing instance for job {name} but launched with different image ({i.image.name}) than requested ({image_name}), terminate {name} first or use new task name."
    result.append(i)

    assert len(result) < 2, f"Found two instances with name {name}"
    if not result:
      return None
    else:
      return result[0]


def lookup_instance(fragment='', valid_states=('running',)):
  """Wrapper around lookup_instances that checks returns a single unambiguous matching instance."""

  instances = lookup_instances(fragment, valid_states=valid_states)
  assert instances, f"Didn't find any instances matching '{fragment}'"
  names = [get_name(i) for i in instances]
  assert len(instances) == 1, f"Found multiple instances matching fragment {fragment}: {','.join(names)}"
  return instances[0]


def lookup_instances(fragment='', *, verbose=True, filter_by_key=False, valid_states=('running',),
                     limit_to_current_user=False) -> List[Instance]:
  """Returns List of ec2.Instance object whose name contains fragment, in reverse order of launching (ie,
  most recent instance first). Optionally filters by key, only including instances launched with
  key_name matching current username.

  If fragment is wrapped in single quotes like 'somemachine', it strips quotes and will try to match somemachine exactly
  args:
    verbose: print information about all matching instances found

    filter_by_key  if True, ignore instances that are not launched with current
        user's default key
    limit_to_current_user Restrict result to instances that current user can ssh into

  """

  if fragment.startswith("'"):
    assert fragment.endswith("'")
    exact_match = True
    fragment = fragment[1:-1]
  else:
    exact_match = False

  def vprint(*args):
    if verbose:
      print(*args)

  region = get_region()
  client = get_ec2_client()
  ec2 = get_ec2_resource()
  response = client.describe_instances()
  assert is_good_response(response)

  instance_list = []
  for instance in ec2.instances.all():
    if instance.state['Name'] not in valid_states:
      continue
    if limit_to_current_user and instance.key_name != get_keypair_name():
      continue

    name = get_name(instance)
    if exact_match:
      if fragment == name:
        instance_list.append((util.toseconds(instance.launch_time), instance))
    else:
      if fragment in name:
        # the following can be re-enabled for broader match
        # or fragment in str(instance.public_ip_address) or
        # fragment in str(instance.id) or fragment in str(instance.private_ip_address)):
        instance_list.append((util.toseconds(instance.launch_time), instance))

  sorted_instance_list = reversed(sorted(instance_list, key=itemgetter(0)))
  filtered_instance_list = []  # filter by key
  vprint("Using region ", region)
  for (ts, instance) in sorted_instance_list:
    if filter_by_key and instance.key_name != get_keypair_name():
      vprint(f"Got key {instance.key_name}, expected {get_keypair_name()}")
      continue
    filtered_instance_list.append(instance)
  return filtered_instance_list


def ssh_to_task(task) -> paramiko.SSHClient:
  """Create ssh connection to task's machine

  returns Paramiko SSH client connected to host.

  """

  username = task.ssh_username
  hostname = task.public_ip
  ssh_key_fn = get_keypair_fn()
  print(f"ssh -i {ssh_key_fn} {username}@{hostname}")
  pkey = paramiko.RSAKey.from_private_key_file(ssh_key_fn)

  ssh_client = paramiko.SSHClient()
  ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  assert ssh_client

  counter = 1
  while True:
    try:
      ssh_client.connect(hostname=hostname, username=username, pkey=pkey)
      if counter % 11 == 0:  # occasionally re-obtain public ip, machine could've gotten restarted
        hostname = task.public_ip
      break
    except Exception as e:
      print(
        f'{task.name}: Exception connecting to {hostname} via ssh (could be a timeout): {e}')
      time.sleep(RETRY_INTERVAL_SEC)

  return ssh_client


def parse_key_name(keyname) -> List[str]:
  """keyname => resource, username"""
  # Relies on resource name not containing -, validated in
  # validate_resource_name
  toks = keyname.split('-')
  if len(toks) != 2:
    return []  # some other keyname not launched by nexus
  else:
    return toks


###################
# Name validation:
# task name is used in machine name so must match AWS resource naming restrictions.
# run name is used in EFS directory name, so use a subset of Posix directory naming requirements. No . because that's used by ncluster naming convention
# prefix is used for both AWS resources and Unix file names so must match both
###################

aws_name_regexp_str = re.compile('^[a-zA-Z0-9+-=._:/@]*$')
aws_name_regexp = re.compile(aws_name_regexp_str)


def validate_aws_name(name) -> None:
  """Validate resource name using AWS name restrictions from # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions"""
  assert len(name) <= 127
  # disallow unicode characters to avoid pain
  assert name == name.encode('ascii').decode('ascii'), f"Non-ascii chars found in '{name}"
  assert aws_name_regexp.match(
    name), f"Creating AWS resource with illegal characters, '{name}' must match regexp '{aws_name_regexp_str}'"


def validate_task_name(name):
  assert name == name.encode('ascii').decode('ascii'), f"Non-ascii chars found in '{name}'"
  # assert '.' not in name, f"can't use . in task name '{name}'"
  assert aws_name_regexp.match(name), f"Task name '{name}' must match regexp '{aws_name_regexp_str}'"


posix_regexp_str = '^[-_a-z0-9]+$'
posix_regexp = re.compile(posix_regexp_str)


def validate_prefix(name):
  """Check that name is valid as substitute for default prefix. Since it's used in unix filenames, key names, be more conservative than AWS requirements, just allow 30 chars, lowercase only."""
  assert len(name) <= 30
  assert posix_regexp.match(name), f"Prefix '{name}' must match '{posix_regexp_str}'"
  validate_aws_name(name)


def validate_run_name(name):
  """Name used for run. Used as part of instance name, tmux session name."""
  assert len(name) <= 30
  assert name == name.encode('ascii').decode('ascii'), f"Non-ascii chars found in '{name}"
  if name:  # allow empty run name
    assert posix_regexp.match(name), f"Run name '{name}' must match '{posix_regexp_str}'"


def create_name_tags(name):
  """Returns [{'Key': 'Name', 'Value': name}] """
  return [{'Key': 'Name', 'Value': name}]


def create_efs(name) -> str:
  efs_client = get_efs_client()
  token = str(int(time.time() * 1e6))  # epoch usec

  response = efs_client.create_file_system(CreationToken=token,
                                           PerformanceMode='generalPurpose')
  assert is_good_response(response)
  start_time = time.time()
  while True:
    try:

      response = efs_client.create_file_system(CreationToken=token,
                                               PerformanceMode='generalPurpose')
      assert is_good_response(response)
      time.sleep(RETRY_INTERVAL_SEC)
    except Exception as e:
      if 'FileSystemAlreadyExists' in str(e):
        break
      if response['Error']['Code'] == 'FileSystemAlreadyExists':
        break
      else:
        util.log_error(e)
      break

    if time.time() - start_time - RETRY_INTERVAL_SEC > RETRY_TIMEOUT_SEC:
      assert False, "Timeout exceeded creating EFS %s (%s)" % (token, name)

    time.sleep(RETRY_INTERVAL_SEC)

  # find efs id from given token
  response = efs_client.describe_file_systems()
  assert is_good_response(response)
  fs_id = extract_attr_for_match(response['FileSystems'], FileSystemId=-1,
                                 CreationToken=token)
  response = efs_client.create_tags(FileSystemId=fs_id,
                                    Tags=create_name_tags(name))
  assert is_good_response(response)

  # make sure EFS is now visible
  efs_dict = get_efs_dict()
  assert name in efs_dict
  return efs_dict[name]


def delete_efs_by_id(efs_id):
  """Deletion sometimes fails, try several times."""
  start_time = time.time()
  efs_client = get_efs_client()
  sys.stdout.write("deleting %s ... " % (efs_id,))
  while True:
    try:
      response = efs_client.delete_file_system(FileSystemId=efs_id)
      if is_good_response(response):
        print("succeeded")
        break
      time.sleep(RETRY_INTERVAL_SEC)
    except Exception as e:
      print("Failed with %s" % (e,))
      if time.time() - start_time - RETRY_INTERVAL_SEC < RETRY_TIMEOUT_SEC:
        print("Retrying in %s sec" % (RETRY_INTERVAL_SEC,))
        time.sleep(RETRY_INTERVAL_SEC)
      else:
        print("Giving up")
        break


def extract_attr_for_match(items, **kwargs):
  """Helper method to get attribute value for an item matching some criterion.
  Specify target criteria value as dict, with target attribute having value -1

  Example:
    to extract state of vpc matching given vpc id

  response = [{'State': 'available', 'VpcId': 'vpc-2bb1584c'}]
  extract_attr_for_match(response, State=-1, VpcId='vpc-2bb1584c') #=> 'available'"""

  # find the value of attribute to return
  query_arg = None
  for arg, value in kwargs.items():
    if value == -1:
      assert query_arg is None, "Only single query arg (-1 valued) is allowed"
      query_arg = arg
  result = []

  filterset = set(kwargs.keys())
  for item in items:
    match = True
    assert filterset.issubset(
      item.keys()), "Filter set contained %s which was not in record %s" % (
      filterset.difference(item.keys()),
      item)
    for arg in item:
      if arg == query_arg:
        continue
      if arg in kwargs:
        if item[arg] != kwargs[arg]:
          match = False
          break
    if match:
      result.append(item[query_arg])
  assert len(result) <= 1, "%d values matched %s, only allow 1" % (
    len(result), kwargs)
  if result:
    return result[0]
  return None


def get_tags(instance):
  """Returns instance tags."""

  return get_instance_property(instance, 'tags')


def get_public_ip(instance):
  return get_instance_property(instance, 'public_ip_address')


def get_ip(instance):
  return get_instance_property(instance, 'private_ip_address')


def get_instance_property(instance, property_name):
  """Retrieves property of an instance, keeps retrying until getting a non-None"""

  name = get_name(instance)
  value = None
  start_time = time.time()
  while True:
    if time.time() - start_time > RETRY_TIMEOUT_SEC:
      assert False, f"Timeout {RETRY_TIMEOUT_SEC} exceeded quering {get_instance_property} on {instance}"
    try:
      value = getattr(instance, property_name)
      if value is not None:
        break
      print(f"retrieving {property_name} on {name} produced None, retrying")
      time.sleep(RETRY_INTERVAL_SEC)
      instance.reload()
      continue
    except Exception as e:
      print(f"retrieving {property_name} on {name} failed with {e}, retrying")
      time.sleep(RETRY_INTERVAL_SEC)
      try:
        instance.reload()
      except Exception:
        pass
      continue

  return value


def call_with_retries(method, debug_string='',
                      retry_interval_sec=RETRY_INTERVAL_SEC,
                      retry_timeout_sec=RETRY_TIMEOUT_SEC,
                      **kwargs):
  start_time = time.time()
  value = None
  while True:
    if time.time() - start_time > retry_timeout_sec:
      assert False, f"Timeout {retry_timeout_sec} exceeded calling {method.__name__}"
    try:
      value = method(**kwargs)
      assert value is not None, f"{debug_string} was None"
      break
    except Exception as e:
      print(f"{debug_string} failed with {e.__class__}({e}), retrying for another {int(retry_timeout_sec - (time.time() - start_time))} seconds")
      time.sleep(retry_interval_sec)
      continue

  return value


def get_ec2_resource() -> EC2_ServiceResource:
  try:
    client = get_session().resource('ec2')
  except Exception as e:
    print(f"Failed with error '{e}'")
    print("To specify Virginia region, do 'export AWS_DEFAULT_REGION=us-east-1'")
    sys.exit()
  return client


def get_ec2_client() -> EC2_Client:
  try:
    client = get_session().client('ec2')
  except Exception as e:
    print(f"Failed with error '{e}'")
    print("To specify Virginia region, do 'export AWS_DEFAULT_REGION=us-east-1'")
    sys.exit()
  return client


def get_efs_client():
  while True:
    try:
      return get_session().client('efs')
    except Exception as e:
      # can get following
      # botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints
      util.log(f"get_session().client('efs') failed with {e}, retrying")
      time.sleep(2)


def is_good_response(response):
  """Helper method to check if boto3 call was a success."""

  code = response["ResponseMetadata"]['HTTPStatusCode']
  # get response code 201 on EFS creation
  return 200 <= code < 300


def get_name(tags_or_instance_or_id):
  """Helper utility to extract name out of tags dictionary or intancce.
      [{'Key': 'Name', 'Value': 'nexus'}] -> 'nexus'
 
     Assert fails if there's more than one name.
     Returns '' if there's less than one name.
  """

  ec2 = get_ec2_resource()
  if hasattr(tags_or_instance_or_id, 'tags'):
    tags = tags_or_instance_or_id.tags
  elif isinstance(tags_or_instance_or_id, str):
    tags = ec2.Instance(tags_or_instance_or_id).tags
  elif tags_or_instance_or_id is None:
    return EMPTY_NAME
  else:
    assert isinstance(tags_or_instance_or_id,
                      Iterable), "expected iterable of tags"
    tags = tags_or_instance_or_id

  if not tags:
    return EMPTY_NAME
  names = [entry['Value'] for entry in tags if entry['Key'] == 'Name']
  if not names:
    return ''
  if len(names) > 1:
    assert False, "have more than one name: " + str(names)
  return names[0]


def wait_until_available(resource):
  """Waits until interval state becomes 'available'"""
  while True:
    resource.load()
    if resource.state == 'available':
      break
    time.sleep(RETRY_INTERVAL_SEC)


def maybe_create_placement_group(name='', max_retries=10):
  """Creates placement_group group or reuses existing one. Crash if unable to create
    placement_group group. If name is empty, ignores request."""

  if not name:
    return

  client = get_ec2_client()
  while True:
    try:
      client.describe_placement_groups(GroupNames=[name])
      print("Reusing placement_group group: " + name)
      break  # no Exception means group name was found
    except Exception:
      print("Creating placement_group group: " + name)
      try:
        _response = client.create_placement_group(GroupName=name,
                                                  Strategy='cluster')
      except botocore.exceptions.ClientError as e:
        if 'PlacementGroupLimitExceeded' in str(e):
          print(e)
          assert False, "Clean-up placement groups using 'ncluster cleanup_placement_groups'"
        print(f"Placement group creation failed with {e}, retrying")
        if e.response['Error']['Code'] == 'InvalidPlacementGroup.Duplicate':
          util.log(f"possible race condition, group {name} already created")
        pass

  counter = 0
  while True:
    try:
      res = client.describe_placement_groups(GroupNames=[name])
      res_entry = res['PlacementGroups'][0]
      if res_entry['State'] == 'available':
        assert res_entry['Strategy'] == 'cluster'
        break
    except Exception as e:
      print("Got exception: %s" % (e,))
    counter += 1
    if counter >= max_retries:
      assert False, f'Failed to create placement_group group {name} in {max_retries} attempts'
    time.sleep(RETRY_INTERVAL_SEC)


# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#concepts-placement-groups
def instance_supports_placement_groups(instance_type: str):
  regex = re.compile(
    "^(m4|m5|m5d|c3|c4|c5|c5d|cc2.8xlarge|cr1.8xlarge|r3|r4|r5|r5d|x1|x1e|z1d|d2|h1|hs1.8xlarge|i2|i3|i3.metal|f1|g2|g3|p2|p3).*$",
    re.IGNORECASE)
  return regex.match(instance_type)


def instance_supports_efa(instance_type: str) -> bool:
  """Checks if instance supports Amazon Elastic Fabric Adapter"""
  # https://docs.aws.amazon.com/en_us/AWSEC2/latest/UserGuide/efa-start.html
  return instance_type in ['c5n.18xlarge', 'i3en.24xlarge', 'p3dn.24xlarge']


def create_spot_instances(launch_specs, spot_price=26, expiration_mins=15):
  """
    args:
      spot_price: default is $26 which is right above p3.16xlarge on demand price
      expiration_mins: this request only valid for this many mins from now
    """
  ec2c = get_ec2_client()

  num_tasks = launch_specs['MinCount'] or 1
  if 'MinCount' in launch_specs:
    del launch_specs['MinCount']
  if 'MaxCount' in launch_specs:
    del launch_specs['MaxCount']
  tags = None
  if 'TagSpecifications' in launch_specs:
    try:
      tags = launch_specs['TagSpecifications'][0]['Tags']
    except Exception:  # Tags entry not present
      pass
    del launch_specs['TagSpecifications']

  import pytz  # datetime is not timezone aware, use pytz to fix
  import datetime as dt
  now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)

  spot_args = {}
  spot_args['LaunchSpecification'] = launch_specs
  spot_args['SpotPrice'] = str(spot_price)
  spot_args['InstanceCount'] = num_tasks
  spot_args['ValidUntil'] = now + dt.timedelta(minutes=expiration_mins)

  try:
    spot_requests = ec2c.request_spot_instances(**spot_args)
  except Exception as e:
    assert False, f"Spot instance request failed (out of capacity?), error was {e}"

  spot_requests = spot_requests['SpotInstanceRequests']
  instance_ids = wait_on_fulfillment(ec2c, spot_requests)

  print('Instances fullfilled...')
  ec2 = get_ec2_resource()
  instances = list(ec2.instances.filter(Filters=[{'Name': 'instance-id', 'Values': list(filter(None, instance_ids))}]))

  if not all(instance_ids):
    for i in instances:
      i.terminate()
    raise RuntimeError('Failed to create spot instances:', instance_ids)

  if tags:
    for i in instances:
      i.create_tags(Tags=tags)

  return instances


def wait_on_fulfillment(ec2c, reqs):
  def get_instance_id(req):
    while req['State'] != 'active':
      print('Waiting on spot fullfillment...')
      time.sleep(5)
      reqs_ = ec2c.describe_spot_instance_requests(
        Filters=[{'Name': 'spot-instance-request-id', 'Values': [req['SpotInstanceRequestId']]}])
      if not reqs_['SpotInstanceRequests']:
        print(f"SpotInstanceRequest for {req['SpotInstanceRequestId']} not found")
        continue
      req = reqs_['SpotInstanceRequests'][0]
      req_status = req['Status']
      if req_status['Code'] not in ['pending-evaluation', 'pending-fulfillment', 'fulfilled']:
        print('Spot instance request failed:', req_status['Message'])
        print('Cancelling request. Please try again or use on demand.')
        ec2c.cancel_spot_instance_requests(SpotInstanceRequestIds=[req['SpotInstanceRequestId']])
        print(req)
        return None
    instance_id = req['InstanceId']
    print('Fulfillment completed. InstanceId:', instance_id)
    return instance_id

  return [get_instance_id(req) for req in reqs]


def assert_zone_specific_config():
  """Make sure user specified zone"""

  assert get_zone(), f"EFA requires launching into specific zone, but zone not specified, try NCLUSTER_ZONE={get_region()}a"


def get_security_group_names() -> List[str]:
  """Returns all security group names associated with given ncluster configuration."""
  return [get_security_group_name(), get_security_group_nd_name()]


def get_efs_mount_command():
  region = get_region()
  efs_id = get_efs_dict()[get_prefix()]
  dns = f"{efs_id}.efs.{region}.amazonaws.com"
  cmd = f'sudo mkdir -p /ncluster && sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dns}:/ /ncluster'
  return cmd


def validate_local_keypair():
  key_name = get_keypair_name()
  keypair_fn = get_keypair_fn()
  if key_name in get_keypair_dict():
    assert os.path.exists(keypair_fn), f"Keypair '{key_name}' exists, but couldn't find corresponding .pem file under '{keypair_fn}'. Either get this file or delete it using 'ncluster fixkeys'"


def running_on_aws() -> bool:
  """
  Returns: True if running on AWS, False otherwise

  """

  # follow recipe from https://serverfault.com/a/575422/88745
  cmd = 'wget -q http://instance-data.ec2.internal && echo yes || echo no'
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  (stdout, stderr) = p.communicate()
  if stderr:
    print(f"Warning, running_on_aws check got stderr '{stderr}'")
    return stdout.decode('ascii')

  if stdout == 'yes':
    return True
  if stdout == 'no':
    return False
  assert False, f"running_on_aws check got '{stdout}' which is neither 'yes' nor 'no'"

