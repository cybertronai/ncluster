#!/usr/bin/env python
#
# Example of two process Ray program, worker sends values to parameter
# server on a different machine
#
# Run locally:
# ./ray_example.py
#
# Run on AWS:
# ./ray_example.py --aws


# Example timings
# c5.18xlarge over network: over network: 113 ms, 840MB/second (6.7 Gbps)
# c5.18xlarge locally: locally: 86 ms, 1218 MB/seconds (9.7 Gbps)
# macbook pro locally: 978.9 ms, 102.15 MB/second

import argparse
import os
import socket
import time

import numpy as np
import ray

parser = argparse.ArgumentParser()
parser.add_argument("--role", default='launcher', type=str,
                    help="launcher/driver")
parser.add_argument('--image', default='Deep Learning AMI (Ubuntu) Version 14.0')
parser.add_argument("--size-mb", default=100, type=int, help='how much data to send at each iteration')
parser.add_argument("--iters", default=100, type=int)
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
parser.add_argument("--xray", default=1, type=int,
                    help="whether to use XRay backend")
parser.add_argument('--nightly', default=1, type=int,
                    help='whether to use nightly version')
parser.add_argument('--macos', default=0, type=int,
                    help='whether we are on Mac')
parser.add_argument('--name', default='ray', type=str,
                    help='name of the run')
parser.add_argument('--instance', default='c5.large', type=str,
                    help='instance type to use')

parser.add_argument("--ip", default='', type=str,
                    help="internal flag, used to point worker to head node")
args = parser.parse_args()

dim = args.size_mb * 250 * 1000


@ray.remote(resources={"worker": 1})
class Worker(object):
  def __init__(self):
    self.gradients = np.ones(dim, dtype=np.float32)

  def compute_gradients(self):
    return self.gradients

  def ip(self):
    return ray.services.get_node_ip_address()


@ray.remote(resources={"ps": 1})
class ParameterServer(object):
  def __init__(self):
    self.params = np.zeros(dim, dtype=np.float32)

  def assign_add(self, grad):
    self.params += grad
    return self.params

  def get_weights(self):
    return self.params

  def ip(self):
    return ray.services.get_node_ip_address()


def run_launcher():
  import ncluster

  if args.aws:
    ncluster.set_backend('aws')

  script = os.path.basename(__file__)
  assert script in os.listdir('.')
  if args.nightly:
    if args.macos:
      install_script = 'pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.5.2-cp36-cp36m-macosx_10_6_intel.whl'
    else:
      install_script = 'pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.5.2-cp36-cp36m-manylinux1_x86_64.whl'
  else:
    install_script = 'pip install ray'
    
  job = ncluster.make_job(name=args.name,
                          install_script=install_script,
                          image_name=args.image,
                          instance_type=args.instance,
                          num_tasks=2)
  job.upload(script)
  if args.xray:
    job.run('export RAY_USE_XRAY=1')
  job.run('ray stop')

  # https://ray.readthedocs.io/en/latest/resources.html?highlight=resources
  ps_resource = """--resources='{"ps": 1}'"""
  worker_resource = """--resources='{"worker": 1}'"""
  ps, worker = job.tasks
  ps.run(f"ray start --head {ps_resource} --redis-port=6379")
  worker.run(f"ray start --redis-address={ps.ip}:6379 {worker_resource}")
  worker.run(f'./{script} --role=driver --ip={ps.ip}:6379 --size-mb={args.size_mb} --iters={args.iters}')


def run_driver():
  ray.init(redis_address=args.ip)

  worker = Worker.remote()
  ps = ParameterServer.remote()
  print(f"Worker ip {ray.get(worker.ip.remote())}")
  print(f"PS ip {ray.get(ps.ip.remote())}")
  print(f"Driver ip {socket.gethostbyname(socket.gethostname())}")

  for iteration in range(args.iters):
    start_time = time.time()
    grads = worker.compute_gradients.remote()
    result = ps.assign_add.remote(grads)
    result = ray.get(result)[0]
    elapsed_time = time.time() - start_time
    rate = args.size_mb / elapsed_time
    print('%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (result, args.iters, args.size_mb, elapsed_time * 1000, rate))


def main():
  if args.role == 'launcher':
    run_launcher()
  elif args.role == 'driver':
    run_driver()
  else:
    assert False, f"Unknown role {args.role}, must be laucher/driver"


if __name__ == '__main__':
  main()
