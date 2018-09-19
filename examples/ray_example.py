#!/usr/bin/env python
#
# Example of two process Ray program, worker sends value to parameter
# server on a different machine
#
# Run locally:
# ./ray_example.py
#
# Run on AWS:
# ./ray_example.py --aws
import argparse
import os

import numpy as np
import ray

parser = argparse.ArgumentParser(description="Run a synchronous parameter "
                                             "server performance benchmark.")
parser.add_argument("--role", default='launcher', type=str,
                    help="launcher/worker/ps")
parser.add_argument('--image', default='Deep Learning AMI (Ubuntu) Version 13.0')
parser.add_argument("--ip", default='', type=str,
                    help="ip of head node")
parser.add_argument("--dim", default=25*1000*1000)
parser.add_argument("--iters", default=10)
parser.add_argument("--aws", action="store_true")

args = parser.parse_args()


@ray.remote(resources={"worker": 1})
class Worker(object):
  def __init__(self):
    self.gradients = np.ones(args.dim, dtype=np.float32)

  def compute_gradients(self):
    return self.gradients

  def ip(self):
    return ray.services.get_node_ip_address()


@ray.remote(resources={"ps": 1})
class ParameterServer(object):
  def __init__(self):
    self.params = np.zeros(args.dim, dtype=np.float32)

  def assign_add(self, grad):
    """Adds all gradients to current value of parameters, returns result."""
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

  if ncluster.get_backend() == 'local':
    os.system('ray stop')

  script = os.path.basename(__file__)
  assert script in os.listdir('.')
  job = ncluster.make_job(install_script='pip install ray',
                          image_name=args.image,
                          instance_type='c5.large',
                          num_tasks=2)
  job.upload(script)
  job.run('export RAY_USE_XRAY=1')

  # https://ray.readthedocs.io/en/latest/resources.html?highlight=resources
  ps_resource = """--resources='{"ps": 1}'"""
  worker_resource = """--resources='{"worker": 1}'"""
  job.tasks[0].run(f"ray start --head {ps_resource} --redis-port=6379")
  job.tasks[1].run(f"ray start --redis-address={job.tasks[0].ip}:6379 {worker_resource}")
  job.tasks[1].run(f'./{script} --role=worker --ip={job.tasks[0].ip}:6379')


def run_ps():

  ray.init(resources={'ps': 1})


def run_worker():
  ray.init(redis_address=args.ip)

  worker = Worker.remote()
  ps = ParameterServer.remote()
  for iteration in range(args.iters):
    grads = worker.compute_gradients.remote()
    result = ps.assign_add.remote(grads)
    print(ray.get(result)[0])


def main():
  if args.role == 'launcher':
    run_launcher()
  elif args.role == 'ps':
    run_ps()
  elif args.role == 'worker':
    run_worker()
  else:
    assert False, f"Unknown role {args.role}, must be worker/launcher/ps"

if __name__ == '__main__':
  main()