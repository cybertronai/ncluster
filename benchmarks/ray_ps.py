#!/usr/bin/env python
#
# Ray parameter server benchmark
#
# python ray_ps.py --aws --num-ps=1 --num-workers=1 --size-mb=100 --iters=100

# # 1 worker, 1 ps
# min:    61.61, median:    63.77, mean:    69.20

# # 1 worker, 2 ps
# python ray_ps.py --aws --num-ps=2 --num-workers=1 --size-mb=100 --iters=100
# min:    49.45, median:    50.91, mean:    58.92

# # 1 worker, 4 ps
# python ray_ps.py --aws --num-ps=4 --num-workers=1 --size-mb=100 --iters=100
# min:    47.98, median:    50.71, mean:    59.05

# # 4 worker, 4 ps
# python ray_ps.py --aws --num-ps=4 --num-workers=4 --size-mb=100 --iters=100
# 098/100 sent 100 MBs in 238.5 ms: 419.28 MB/second
# 099/100 sent 100 MBs in 242.0 ms: 413.22 MB/second
# min:   219.90, median:   241.51, mean:   245.95
# (54ms per worker since 4x more work done)

# # 1 worker, 4 ps, larger arrays
# python ray_ps.py --aws --num-ps=4 --num-workers=1 --size-mb=800 --iters=100
# min:   358.35, median:   544.59, mean:   513.47
#
# Bottom line, 50-60ms to send 100MB regardless of sharding/workers

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
parser.add_argument("--size-mb", default=10, type=int,
                    help='how much data to send at each iteration')
parser.add_argument("--num-workers", default=2, type=int)
parser.add_argument("--num-ps", default=2, type=int)

parser.add_argument("--iters", default=11, type=int)
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
parser.add_argument("--xray", default=1, type=int,
                    help="whether to use XRay backend")
parser.add_argument('--nightly', default=1, type=int,
                    help='whether to use nightly version')
parser.add_argument('--name', default='ray_ps', type=str,
                    help='name of the run')
parser.add_argument("--ip", default='', type=str,
                    help="internal flag, used to point worker to head node")
args = parser.parse_args()

dim = args.size_mb * 250 * 1000 // args.num_ps


@ray.remote(resources={"worker": 1})
class Worker(object):
  def __init__(self):
    self.gradients = np.ones(dim, dtype=np.float32)

  @ray.method(num_return_vals=args.num_ps)
  def compute_gradients(self):
    if args.num_ps == 1:
      return self.gradients
    return [self.gradients]*args.num_ps

  def ip(self):
    return ray.services.get_node_ip_address()


@ray.remote(resources={"worker": 1})
class ParameterServer(object):
  def __init__(self):
    self.params = np.zeros(dim, dtype=np.float32)

  def receive(self, *grad_list):
    for grad in grad_list:
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
                          num_tasks=args.num_workers+args.num_ps)
  if not ncluster.running_locally():
    job._run_raw('killall python', ignore_errors=True)
  
  job.upload(__file__)
  job.upload('util.py')
  if args.xray:
    job.run('export RAY_USE_XRAY=1')
  job.run('ray stop')

  head = job.tasks[0]

  # https://ray.readthedocs.io/en/latest/resources.html?highlight=resources
  worker_resource = """--resources='{"worker": 1}'"""
  head.run(f"ray start --head {worker_resource} --redis-port=6379")

  for task in job.tasks[1:]:
    task.run(f"ray start --redis-address={head.ip}:6379 {worker_resource}")
  
  head.run(f'python {__file__} --role=driver --ip={head.ip}:6379 --size-mb={args.size_mb} --iters={args.iters} --num-workers={args.num_workers} --num-ps={args.num_ps}')
  
  print(head.file_read('out'))


def transpose(list_of_lists):
  return list(map(list, zip(*list_of_lists)))


def run_driver():
  ray.init(redis_address=args.ip)

  worker_actors = [Worker.remote() for _ in range(args.num_workers)]
  ps_actors = [ParameterServer.remote() for _ in range(args.num_ps)]
  
  log = util.FileLogger('out')

  time_list = []
  for i in range(args.iters):
    start_time = time.perf_counter()
    grads_list = []
    for actor in worker_actors:
      result = actor.compute_gradients.remote()
      if args.num_ps == 1:
        grads_list.append([result])
      else:
        grads_list.append(result)
    
    updates = []
    for ps, shards in zip(ps_actors, transpose(grads_list)):
      updates.append(ps.receive.remote(*shards))
    
    ray.wait(updates, num_returns=args.num_ps)
    
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
