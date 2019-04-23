#!/usr/bin/env python
#
# Deletes resources

import sys
import os
import argparse

from ncluster import aws_util as u
from ncluster import util

parser = argparse.ArgumentParser()
parser.add_argument('--kind', type=str, default='all',
                    help="which resources to delete, all/network/keypair/efs")
parser.add_argument('--force-delete-efs', action='store_true',
                    help="force deleting main EFS")
args = parser.parse_args()

EFS_NAME = u.get_prefix()
VPC_NAME = u.get_prefix()
SECURITY_GROUP_NAME = u.get_prefix()
ROUTE_TABLE_NAME = u.get_prefix()
KEYPAIR_NAME = u.get_keypair_name()

client = u.get_ec2_client()
ec2 = u.get_ec2_resource()


def response_type(response):
  return 'ok' if u.is_good_response(response) else 'failed'


def delete_efs():
  efss = u.get_efs_dict()
  efs_id = efss.get(EFS_NAME, '')
  efs_client = u.get_efs_client()
  if efs_id:
    try:
      # delete mount targets first
      print("About to delete %s (%s)" % (efs_id, EFS_NAME))
      response = efs_client.describe_mount_targets(FileSystemId=efs_id)
      assert u.is_good_response(response)
      for mount_response in response['MountTargets']:
        id_ = mount_response['MountTargetId']
        sys.stdout.write('Deleting mount target %s ... ' % (id_,))
        sys.stdout.flush()
        response = efs_client.delete_mount_target(MountTargetId=id_)
        print(response_type(response))

      sys.stdout.write('Deleting EFS %s (%s)... ' % (efs_id, EFS_NAME))
      sys.stdout.flush()
      u.delete_efs_by_id(efs_id)

    except Exception as e:
      sys.stdout.write(f'failed with {e}\n')
      util.log_error(str(e) + '\n')


def delete_network():
  existing_vpcs = u.get_vpc_dict()
  if VPC_NAME in existing_vpcs:
    vpc = ec2.Vpc(existing_vpcs[VPC_NAME].id)
    print("Deleting VPC %s (%s) subresources:" % (VPC_NAME, vpc.id))

    for subnet in vpc.subnets.all():
      try:
        sys.stdout.write("Deleting subnet %s ... " % subnet.id)
        sys.stdout.write(response_type(subnet.delete()) + '\n')
      except Exception as e:
        sys.stdout.write('failed\n')
        util.log_error(str(e) + '\n')

    for gateway in vpc.internet_gateways.all():
      sys.stdout.write("Deleting gateway %s ... " % gateway.id)
      # note: if instances are using VPC, this fails with
      # botocore.exceptions.ClientError: An error occurred (DependencyViolation) when calling the DetachInternetGateway operation: Network vpc-ca4abab3 has some mapped public address(es). Please unmap those public address(es) before detaching the gateway.

      sys.stdout.write('detached ... ' if u.is_good_response(
        gateway.detach_from_vpc(VpcId=vpc.id)) else ' detach_failed ')
      sys.stdout.write('deleted ' if u.is_good_response(
        gateway.delete()) else ' delete_failed ')
      sys.stdout.write('\n')

    def desc():
      return "%s (%s)" % (route_table.id, u.get_name(route_table.tags))

    for route_table in vpc.route_tables.all():
      sys.stdout.write(f"Deleting route table {desc()} ... ")
      try:
        sys.stdout.write(response_type(route_table.delete()) + '\n')
      except Exception as e:
        sys.stdout.write('failed\n')
        util.log_error(str(e) + '\n')

    def desc():
      return "%s (%s, %s)" % (
        security_group.id, u.get_name(security_group.tags),
        security_group.group_name)

    for security_group in vpc.security_groups.all():
      # default group is undeletable, skip
      if security_group.group_name == 'default':
        continue
      sys.stdout.write(
        'Deleting security group %s ... ' % (desc()))
      try:
        sys.stdout.write(response_type(security_group.delete()) + '\n')
      except Exception as e:
        sys.stdout.write('failed\n')
        util.log_error(str(e) + '\n')

    sys.stdout.write("Deleting VPC %s ... " % vpc.id)
    try:
      sys.stdout.write(response_type(vpc.delete()) + '\n')
    except Exception as e:
      sys.stdout.write('failed\n')
      util.log_error(str(e) + '\n')


def delete_keypair():
  keypairs = u.get_keypair_dict()
  keypair = keypairs.get(KEYPAIR_NAME, '')
  if keypair:
    try:
      sys.stdout.write("Deleting keypair %s (%s) ... " % (keypair.key_name,
                                                          KEYPAIR_NAME))
      sys.stdout.write(response_type(keypair.delete()) + '\n')
    except Exception as e:
      sys.stdout.write('failed\n')
      util.log_error(str(e) + '\n')

  keypair_fn = u.get_keypair_fn()
  if os.path.exists(keypair_fn):
    print("Deleting local keypair file %s" % (keypair_fn,))
    os.system('rm -f ' + keypair_fn)


def delete_resources(force_delete_efs=False):
  region = os.environ['AWS_DEFAULT_REGION']

  resource = u.get_prefix()
  print(f"Deleting {resource} resources in region {region}")
  print(f"Make sure {resource} instances are terminated or this will fail.")

  if 'efs' in args.kind or 'all' in args.kind:
    if EFS_NAME == u.DEFAULT_PREFIX and not force_delete_efs:
      # this is default EFS, likely has stuff, require extra flag to delete it
      print("default EFS has useful stuff in it, not deleting it. Use force-delete-efs "
            "flag to force")
    else:
      delete_efs()
  if 'network' in args.kind or 'all' in args.kind:
    delete_network()
  if 'keypair' in args.kind or 'all' in args.kind:
    delete_keypair()


if __name__ == '__main__':
  delete_resources(force_delete_efs=args.force_delete_efs)
