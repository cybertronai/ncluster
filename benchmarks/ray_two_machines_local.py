#!/usr/bin/env python
#
# Runs two machine benchmark locally on AWS machine
#
# Example timings
# macbook: added 10 MBs in 14.1 ms: 707.68 MB/second
# c5.18xlarge: added 10 MBs in 4.4 ms: 2298.82 MB/second
#      091/100 added 100 MBs in 30.8 ms: 3246.44 MB/second

# Bottom line: can do 3.2 GB/second running locally, 800 
import argparse
import os
import socket
import subprocess
import time

import numpy as np
import ray

import util

parser = argparse.ArgumentParser()
parser.add_argument('--image',
                    default='Deep Learning AMI (Ubuntu) Version 15.0')
parser.add_argument("--size-mb", default=100, type=int,
                    help='how much data to send at each iteration')
parser.add_argument("--iters", default=11, type=int)
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
parser.add_argument("--xray", default=1, type=int,
                    help="whether to use XRay backend")
parser.add_argument('--nightly', default=1, type=int,
                    help='whether to use nightly version')
parser.add_argument('--name', default='ray_two_machines', type=str,
                    help='name of the run')

parser.add_argument("--ip", default='', type=str,
                    help="internal flag, used to point worker to head node")
parser.add_argument("--role", default='launcher', type=str,
                    help="interanl flag, launcher/driver")
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
    self.params = grad  # use = just to get network overhead
    return self.params

  def get_weights(self):
    return self.params

  def ip(self):
    return ray.services.get_node_ip_address()



def run_launcher():
  import ncluster

  if args.aws:
    ncluster.set_backend('aws')

  if args.nightly:
    # running locally MacOS
    if 'Darwin' in util.ossystem('uname') and not args.aws:
      install_script = 'pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.5.2-cp36-cp36m-macosx_10_6_intel.whl'
    else:
      install_script = 'pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.5.2-cp36-cp36m-manylinux1_x86_64.whl'
  else:
    install_script = 'pip install ray'

  worker = ncluster.make_task(name=args.name,
                            install_script=install_script,
                            image_name=args.image)
  if not ncluster.running_locally():
    worker._run_raw('killall python', ignore_errors=True)
  worker.upload(__file__)
  worker.upload('util.py')
  if args.xray:
    worker.run('export RAY_USE_XRAY=1')
  worker.run('ray stop')

  resources = """--resources='{"ps": 1, "worker": 1}'"""
  worker.run(f"ray start --head {resources} --redis-port=6379")
  #  worker.run(f"ray start --redis-address={worker.ip}:6379 {resources}")
  worker.run(
    f'./{__file__} --role=driver --ip={worker.ip}:6379 --size-mb={args.size_mb} --iters={args.iters}')
  print(worker.read('out'))


def run_driver():
  ray.init(redis_address=args.ip)

  worker = Worker.remote()
  ps = ParameterServer.remote()
  log = util.FileLogger('out')
  log(f"Worker ip {ray.get(worker.ip.remote())}")
  log(f"Driver ip {socket.gethostbyname(socket.gethostname())}")

  time_list = []
  for i in range(args.iters):
    start_time = time.perf_counter()
    grads = worker.compute_gradients.remote()
    result = ps.assign_add.remote(grads)
    result = ray.get(result)[0]
    elapsed_time_ms = (time.perf_counter() - start_time)*1000
    time_list.append(elapsed_time_ms)
    rate = args.size_mb / (elapsed_time_ms/1000)
    log('%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (i, args.iters, args.size_mb, elapsed_time_ms, rate))
    
  min = np.min(time_list)
  median = np.median(time_list)
  log(f"min: {min:8.2f}, median: {median:8.2f}, mean: {np.mean(time_list):8.2f}")


def main():
  if args.role == 'launcher':
    run_launcher()
  elif args.role == 'driver':
    run_driver()
  else:
    assert False, f"Unknown role {args.role}, must be laucher/driver"


if __name__ == '__main__':
  main()
