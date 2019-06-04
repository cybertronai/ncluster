#!/usr/bin/env python
#
# Creates resources.
# To run standalone:
# python -m ncluster.aws_create_resources
#
# This script creates VPC/security group/keypair if not already present

import os
import sys
import time
from typing import Tuple, Any, Optional

from boto3_type_annotations.ec2 import SecurityGroup

from ncluster import aws_util as u
from ncluster import util

DRYRUN = False
DEBUG = True

# Names of Amazon resources that are created. These settings are fixed across
# all runs, and correspond to resources created once per user per region.

NFS_PORT = 2049
PUBLIC_TCP_RANGES = [
  22,  # ssh
  NFS_PORT,  # NFS port NFS peering between security groups
  # ipython notebook ports
  (8888, 8899),
  # redis port
  6379,
  # tensorboard ports
  (6006, 6016)
]

PUBLIC_UDP_RANGES = [NFS_PORT, (60000, 61000)]  # mosh ports


def network_setup() -> Tuple[Any, Any]:
  """Creates VPC if it doesn't already exists, configures it for public
  internet access, returns vpc, subnet, security_group"""

  # from https://gist.github.com/nguyendv/8cfd92fc8ed32ebb78e366f44c2daea6

  ec2 = u.get_ec2_resource()
  client = u.get_ec2_client()
  existing_vpcs = u.get_vpc_dict()
  zones = u.get_zones()

  # create VPC from scratch. Remove this if default VPC works well enough.
  create_non_default_vpc = False

  if create_non_default_vpc:
    vpc_name = u.get_vpc_name()
    if u.get_vpc_name() in existing_vpcs:
      print("Reusing VPC " + vpc_name)
      vpc = existing_vpcs[vpc_name]
      subnets = list(vpc.subnets.all())
      assert len(subnets) == len(
        zones), "Has %s subnets, but %s zones, something went wrong during resource creation, try delete_resources.py/create_resources.py" % (
        len(subnets), len(zones))

    else:
      print("Creating VPC " + vpc_name)
      vpc = ec2.create_vpc(CidrBlock='192.168.0.0/16')

      # enable DNS on the VPC
      response = vpc.modify_attribute(EnableDnsHostnames={"Value": True})
      assert u.is_good_response(response)
      response = vpc.modify_attribute(EnableDnsSupport={"Value": True})
      assert u.is_good_response(response)

      vpc.create_tags(Tags=u.create_name_tags(vpc_name))
      vpc.wait_until_available()

    gateways = u.get_gateway_dict(vpc)
    gateway_name = u.get_gateway_name()
    if gateway_name in gateways:
      print("Reusing gateways " + gateway_name)
    else:
      print("Creating internet gateway " + gateway_name)
      ig = ec2.create_internet_gateway()
      ig.attach_to_vpc(VpcId=vpc.id)
      ig.create_tags(Tags=u.create_name_tags(gateway_name))

      # check that attachment succeeded
      attach_state = u.extract_attr_for_match(ig.attachments, State=-1,
                                              VpcId=vpc.id)
      assert attach_state == 'available', "vpc %s is in state %s" % (vpc.id,
                                                                     attach_state)
      route_table = vpc.create_route_table()
      route_table_name = u.get_route_table_name()
      route_table.create_tags(Tags=u.create_name_tags(route_table_name))

      dest_cidr = '0.0.0.0/0'
      route_table.create_route(
        DestinationCidrBlock=dest_cidr,
        GatewayId=ig.id
      )
      # check success
      for route in route_table.routes:
        # result looks like this
        # ec2.Route(route_table_id='rtb-a8b438cf',
        #    destination_cidr_block='0.0.0.0/0')
        if route.destination_cidr_block == dest_cidr:
          break
      else:
        # sometimes get
        #      AssertionError: Route for 0.0.0.0/0 not found in [ec2.Route(route_table_id='rtb-cd9153b0', destination_cidr_block='192.168.0.0/16')]
        # TODO: add a wait/retry?
        assert False, "Route for %s not found in %s" % (dest_cidr,
                                                        route_table.routes)

      assert len(zones) <= 16  # for cidr/20 to fit into cidr/16
      ip = 0
      for zone in zones:
        cidr_block = '192.168.%d.0/20' % (ip,)
        ip += 16
        print("Creating subnet %s in zone %s" % (cidr_block, zone))
        subnet = vpc.create_subnet(CidrBlock=cidr_block,
                                   AvailabilityZone=zone)
        subnet.create_tags(Tags=[{'Key': 'Name', 'Value': f'{vpc_name}-subnet'},
                                 {'Key': 'Region', 'Value': zone}])
        response = client.modify_subnet_attribute(
          MapPublicIpOnLaunch={'Value': True},
          SubnetId=subnet.id
        )
        assert u.is_good_response(response)
        u.wait_until_available(subnet)
        assert subnet.map_public_ip_on_launch, "Subnet doesn't enable public IP by default, why?"

        route_table.associate_with_subnet(SubnetId=subnet.id)

  #  Setup security group for non-default VPC
  #  existing_security_groups = u.get_security_group_dict()
  #  security_group_nd_name = u.get_security_group_nd_name()
  # if security_group_nd_name in existing_security_groups:
  #   print("Reusing non-default security group " + security_group_nd_name)
  #   security_group_nd = existing_security_groups[security_group_nd_name]
  #   assert security_group_nd.vpc_id == vpc.id, f"Found non-default security group {security_group_nd} " \
  #                                              f"attached to {security_group_nd.vpc_id} but expected {vpc.id}"
  # else:
  #   security_group_nd = create_security_group(security_group_nd_name, vpc.id)

  # Setup things on default VPC for zone-agnostic launching
  vpc = u.get_default_vpc()
  if not vpc:
    util.log(f"Creating default VPC for region {u.get_region()}")
    client.create_default_vpc()
  vpc = u.get_default_vpc()
  assert vpc, "Could not create default VPC?"

  existing_security_groups = u.get_security_group_dict()
  security_group_name = u.get_security_group_name()
  if security_group_name in existing_security_groups:
    print("Reusing security group " + security_group_name)
    security_group = existing_security_groups[security_group_name]
    assert security_group.vpc_id == vpc.id, f"Found security group {security_group} " \
                                            f"attached to {security_group.vpc_id} but expected {vpc.id}"
  else:
    security_group = create_security_group(security_group_name, vpc.id)

  return vpc, security_group


def keypair_setup():
  """Creates keypair if necessary, saves private key locally, returns contents
  of private key file."""

  os.system('mkdir -p ' + u.PRIVATE_KEY_LOCATION)

  keypair_name = u.get_keypair_name()
  keypair = u.get_keypair_dict().get(keypair_name, None)
  keypair_fn = u.get_keypair_fn()
  if keypair:
    print("Reusing keypair " + keypair_name)
    # check that local pem file exists and is readable
    assert os.path.exists(
      keypair_fn), "Keypair %s exists, but corresponding .pem file %s is not found, delete keypair %s through console and run again to recreate keypair/.pem together" % (
      keypair_name, keypair_fn, keypair_name)
    keypair_contents = open(keypair_fn).read()
    assert len(keypair_contents) > 0
  else:
    print("Creating keypair " + keypair_name)
    ec2 = u.get_ec2_resource()
    assert not os.path.exists(
      keypair_fn), "previous keypair exists, delete it with 'sudo rm %s' and also delete corresponding keypair through console" % (
      keypair_fn)
    keypair = ec2.create_key_pair(KeyName=keypair_name)

    open(keypair_fn, 'w').write(keypair.key_material)
    os.system('chmod 400 ' + keypair_fn)

  return keypair


def placement_group_setup(group_name):
  """Creates placement_group group if necessary. Returns True if new placement_group
  group was created, False otherwise."""

  existing_placement_groups = u.get_placement_group_dict()

  group = existing_placement_groups.get(group_name, None)
  if group:
    assert group.state == 'available'
    assert group.strategy == 'cluster'
    print("Reusing group ", group.name)
    return group

  print("Creating group " + group_name)
  ec2 = u.get_ec2_resource()
  group = ec2.create_placement_group(GroupName=group_name, Strategy='cluster')
  return group


def create_security_group(security_group_name: str, vpc_id: str, other_group: Optional[SecurityGroup] = None):
  """Creates security group with proper ports open. Optionally allows all traffic from other_group"""
  print("Creating security group " + security_group_name)
  ec2 = u.get_ec2_resource()

  security_group: SecurityGroup = ec2.create_security_group(
    GroupName=security_group_name, Description=security_group_name,
    VpcId=vpc_id)

  security_group.create_tags(Tags=u.create_name_tags(security_group_name))

  # allow ICMP access for public ping
  security_group.authorize_ingress(
    CidrIp='0.0.0.0/0',
    IpProtocol='icmp',
    FromPort=-1,
    ToPort=-1
  )

  # open public ports
  # always include SSH port which is required for basic functionality
  assert 22 in PUBLIC_TCP_RANGES, "Must enable SSH access"
  for port in PUBLIC_TCP_RANGES:
    if util.is_iterable(port):
      assert len(port) == 2
      from_port, to_port = port
    else:
      from_port, to_port = port, port

    response = security_group.authorize_ingress(IpProtocol="tcp",
                                                CidrIp="0.0.0.0/0",
                                                FromPort=from_port,
                                                ToPort=to_port)
    assert u.is_good_response(response)

  for port in PUBLIC_UDP_RANGES:
    if util.is_iterable(port):
      assert len(port) == 2
      from_port, to_port = port
    else:
      from_port, to_port = port, port

    response = security_group.authorize_ingress(IpProtocol="udp",
                                                CidrIp="0.0.0.0/0",
                                                FromPort=from_port,
                                                ToPort=to_port)
    assert u.is_good_response(response)

  # allow ingress within security group
  # Authorizing ingress doesn't work with names in a non-default VPC,
  # so must use more complicated syntax
  # https://github.com/boto/boto3/issues/158
  def authorize_from_group(security_group_: SecurityGroup, other_group_: SecurityGroup):
    response_ = {}
    for protocol in ['icmp']:
      try:
        rule = {'FromPort': -1,
                'IpProtocol': protocol,
                'IpRanges': [],
                'PrefixListIds': [],
                'ToPort': -1,
                'UserIdGroupPairs': [{'GroupId': other_group_.id}]}
        response_ = security_group_.authorize_ingress(IpPermissions=[rule])
      except Exception as e:
        if response_['Error']['Code'] == 'InvalidPermission.Duplicate':
          print("Warning, got " + str(e))
        else:
          assert False, "Failed while authorizing ingress with " + str(e)

    for protocol in ['tcp', 'udp']:
      try:
        rule = {'FromPort': 0,
                'IpProtocol': protocol,
                'IpRanges': [],
                'PrefixListIds': [],
                'ToPort': 65535,
                'UserIdGroupPairs': [{'GroupId': other_group_.id}]}
        response_ = security_group_.authorize_ingress(IpPermissions=[rule])
      except Exception as e:
        if response_['Error']['Code'] == 'InvalidPermission.Duplicate':
          print("Warning, got " + str(e))
        else:
          assert False, "Failed while authorizing ingress with " + str(e)

  authorize_from_group(security_group, security_group)
  if other_group:
    authorize_from_group(security_group, other_group)

  return security_group


def create_resources():
  print(f"Creating {u.get_prefix()} resources in region {u.get_region()}")

  vpc, security_group = network_setup()
  keypair_setup()  # saves private key locally to keypair_fn

  # create EFS
  efss = u.get_efs_dict()
  efs_name = u.get_efs_name()
  efs_id = efss.get(efs_name, '')
  if not efs_id:
    print("Creating EFS " + efs_name)
    efs_id = u.create_efs(efs_name)
  else:
    print("Reusing EFS " + efs_name)

  efs_client = u.get_efs_client()

  # create mount target for each subnet in the VPC

  # added retries because efs is not immediately available
  max_failures = 10
  retry_interval_sec = 1
  for subnet in vpc.subnets.all():
    for retry_attempt in range(max_failures):
      try:
        sys.stdout.write(
          "Creating efs mount target for %s ... " % (subnet.availability_zone,))
        sys.stdout.flush()
        response = efs_client.create_mount_target(FileSystemId=efs_id,
                                                  SubnetId=subnet.id,
                                                  SecurityGroups=[
                                                    security_group.id])
        if u.is_good_response(response):
          print("success")
          break
      except Exception as e:
        if 'already exists' in str(e):  # ignore "already exists" errors
          print('already exists')
          break

        # Takes couple of seconds for EFS to come online, with
        # errors like this:
        # Creating efs mount target for us-east-1f ... Failed with An error occurred (IncorrectFileSystemLifeCycleState) when calling the CreateMountTarget operation: None, retrying in 1 sec

        print("Got %s, retrying in %s sec" % (str(e), retry_interval_sec))
        time.sleep(retry_interval_sec)
    else:
      print("Giving up.")


if __name__ == '__main__':
  create_resources()
