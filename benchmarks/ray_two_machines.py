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

import argparse
import os
import time

import numpy as np
import ray

parser = argparse.ArgumentParser()
parser.add_argument("--role", default='launcher', type=str,
                    help="launcher/driver")
parser.add_argument('--image', default='Deep Learning AMI (Ubuntu) Version 13.0')
parser.add_argument("--size-mb", default=10, type=int, help='how much data to send at each iteration')
parser.add_argument("--iters", default=10, type=int)
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
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


@ray.remote(resources={"ps": 1})
class ParameterServer(object):
  def __init__(self):
    self.params = np.zeros(dim, dtype=np.float32)

  def assign_add(self, grad):
    self.params += grad
    return self.params

  def get_weights(self):
    return self.params


def run_launcher():
  import ncluster

  if args.aws:
    ncluster.set_backend('aws')

  script = os.path.basename(__file__)
  assert script in os.listdir('.')
  job = ncluster.make_job(install_script='pip install ray',
                          image_name=args.image,
                          instance_type='c5.large',
                          num_tasks=2)
  job.upload(script)
  job.run('export RAY_USE_XRAY=1')
  job.run('ray stop')

  # https://ray.readthedocs.io/en/latest/resources.html?highlight=resources
  ps_resource = """--resources='{"ps": 1}'"""
  worker_resource = """--resources='{"worker": 1}'"""
  ps, worker = job.tasks
  common=''  #" --object-store-memory=5000000000"
  ps.run(f"ray start --head {ps_resource} --redis-port=6379"+common)
  worker.run(f"ray start --redis-address={ps.ip}:6379 {worker_resource}"+common)
  worker.run(f'./{script} --role=driver --ip={ps.ip}:6379 --size-mb={args.size_mb} --iters={args.iters}')


def run_driver():
  ray.init(redis_address=args.ip)

  worker = Worker.remote()
  ps = ParameterServer.remote()

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
