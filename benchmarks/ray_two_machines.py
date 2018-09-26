#!/usr/bin/env python
#
# Example of two process Ray program, worker sends values to parameter
# server on a different machine
#
# Run locally:
# ./ray_two_machines.py
#
# Run on AWS:
# ./ray_two_machines.py --aws


# Example timings
# c5.18xlarge over network: over network: 63.0 ms: 1586.76 MB/second
# c5.9xlarge over network: 399/400 added 100 MBs in 85.5 ms: 1170.26 MB/second
# c5.18xlarge locally: 86 ms, 1218 MB/seconds (9.7 Gbps)
# macbook pro locally: 978.9 ms, 102.15 MB/second

# c5.18xlarge
# 004/11 sent 100 MBs in 69.4 ms: 1440.31 MB/second
# 005/11 sent 100 MBs in 68.1 ms: 1468.95 MB/second
# 006/11 sent 100 MBs in 70.4 ms: 1421.40 MB/second
# 007/11 sent 100 MBs in 69.5 ms: 1438.62 MB/second
# 008/11 sent 100 MBs in 66.4 ms: 1506.90 MB/second
# 009/11 sent 100 MBs in 76.5 ms: 1306.92 MB/second
# 010/11 sent 100 MBs in 66.8 ms: 1497.64 MB/second
# min:    66.36, median:    69.43, mean:    70.55

# Another run
# 989/1000 sent 100 MBs in 54.6 ms: 1831.07 MB/second
# 990/1000 sent 100 MBs in 54.4 ms: 1837.20 MB/second
# 991/1000 sent 100 MBs in 54.8 ms: 1824.91 MB/second
# 992/1000 sent 100 MBs in 53.4 ms: 1874.39 MB/second
# 993/1000 sent 100 MBs in 53.1 ms: 1881.77 MB/second
# 994/1000 sent 100 MBs in 52.7 ms: 1897.76 MB/second
# 995/1000 sent 100 MBs in 55.4 ms: 1805.42 MB/second
# 996/1000 sent 100 MBs in 53.4 ms: 1872.93 MB/second
# 997/1000 sent 100 MBs in 52.7 ms: 1896.65 MB/second
# 998/1000 sent 100 MBs in 54.0 ms: 1851.14 MB/second
# 999/1000 sent 100 MBs in 53.6 ms: 1864.93 MB/second
# min:    51.11, median:    55.45, mean:    60.74


# Bottom line: 30ms locally, 60ms over network

import argparse
import os
import socket
import subprocess
import time

import numpy as np
import ray

import util

parser = argparse.ArgumentParser()
parser.add_argument("--role", default='launcher', type=str,
                    help="launcher/driver")
parser.add_argument('--image',
                    default='Deep Learning AMI (Ubuntu) Version 14.0')
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

  def receive(self, grad):
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

  job = ncluster.make_job(name=args.name,
                          install_script=install_script,
                          image_name=args.image,
                          num_tasks=2)
  ps, worker = job.tasks
  if not ncluster.running_locally():
    ps._run_raw('killall python', ignore_errors=True)
    worker._run_raw('killall python', ignore_errors=True)
  
  job.upload(__file__)
  job.upload('util.py')
  job.run('pip install ray')
  if args.xray:
    job.run('export RAY_USE_XRAY=1')
  job.run('ray stop')

  # https://ray.readthedocs.io/en/latest/resources.html?highlight=resources
  ps_resource = """--resources='{"ps": 1}'"""
  worker_resource = """--resources='{"worker": 1}'"""
  
  ps.run(f"ray start --head {ps_resource} --redis-port=6379")
  worker.run(f"ray start --redis-address={ps.ip}:6379 {worker_resource}")
  worker.run(
    f'./{__file__} --role=driver --ip={ps.ip}:6379 --size-mb={args.size_mb} --iters={args.iters}')
  print(worker.file_read('out'))


def run_driver():
  ray.init(redis_address=args.ip)

  worker = Worker.remote()
  ps = ParameterServer.remote()
  log = util.FileLogger('out')
  log(f"Worker ip {ray.get(worker.ip.remote())}")
  log(f"PS ip {ray.get(ps.ip.remote())}")
  log(f"Driver ip {socket.gethostbyname(socket.gethostname())}")

  time_list = []
  for i in range(args.iters):
    start_time = time.perf_counter()
    grads = worker.compute_gradients.remote()
    result = ps.receive.remote(grads)
    ray.wait([result])
    elapsed_time_ms = (time.perf_counter() - start_time)*1000
    time_list.append(elapsed_time_ms)
    rate = args.size_mb / (elapsed_time_ms/1000)
    log('%03d/%d sent %d MBs in %.1f ms: %.2f MB/second' % (i, args.iters, args.size_mb, elapsed_time_ms, rate))
    
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
