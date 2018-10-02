#!/usr/bin/env python

"""
Running locally

004/11 sent 100 MBs in 28.4 ms: 3519.33 MB/second
005/11 sent 100 MBs in 25.1 ms: 3988.50 MB/second
006/11 sent 100 MBs in 25.5 ms: 3918.33 MB/second
007/11 sent 100 MBs in 25.3 ms: 3958.61 MB/second
008/11 sent 100 MBs in 25.3 ms: 3954.15 MB/second
009/11 sent 100 MBs in 24.9 ms: 4009.78 MB/second
010/11 sent 100 MBs in 25.0 ms: 3992.75 MB/second
min:    24.94, median:    25.52, mean:    29.53


"""

import argparse
import json
import os
import numpy as np
import tensorflow as tf
import time

import util

parser = argparse.ArgumentParser()
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
parser.add_argument("--iters", default=11, type=int,
                    help="Maximum number of additions")
parser.add_argument("--size-mb", default=100, type=int,
                    help="size of vector in MBs")
parser.add_argument("--shards", default=1, type=int,
                    help="how many ways to shard the variable")
parser.add_argument('--image',
                    default='Deep Learning AMI (Ubuntu) Version 15.0')
parser.add_argument('--name',
                    default='mpi')

# internal flags
parser.add_argument('--role', default='launcher', type=str)
args = parser.parse_args()


def run_launcher():
  import ncluster
  if args.aws:
    ncluster.set_backend('aws')

  job = ncluster.make_job(args.name, num_tasks=2, image_name=args.image)
  job.upload(__file__)
  job.upload('util.py')

  # kill python just for when tmux session reuse is on
  if not ncluster.running_locally():
    job._run_raw('killall python', ignore_errors=True)

  if ncluster.get_backend() == 'aws':
    # on AWS probably running in conda DLAMI, switch into TF-enabled env
    job.run('source activate tensorflow_p36')

  
  hosts = [task.public_ip for task in job.tasks]
  host_str = ','.join(hosts)
  os.system(f'mpirun -np 2 --host {host_str} python {__file__} --role=worker')
  print(job.tasks[0].read('/tmp/out'))


def run_worker():
  param_size = 250 * 1000 * args.size_mb // args.shards  # 1MB is 250k integers

  from mpi4py import MPI
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()

  if rank == 0:
    log = util.FileLogger('/tmp/out')
    #    log = util.FileLogger('/dev/null', mirror=False)

  else:
    log = util.FileLogger('/dev/null', mirror=False)
  grads_array = []

  time_list = []
  dim = args.size_mb*250*1000
  dtype = np.float32
  data = np.ones(dim, dtype=dtype)*(rank+1)
  for i in range(args.iters):
    start_time = time.perf_counter()
    if rank == 0:
      comm.Send(data, dest=1, tag=13)
    else:
      data = np.empty(dim, dtype=dtype)
      comm.Recv(data, source=0, tag=13)
      
    end_time = time.perf_counter()
    
    elapsed_time_ms = (end_time - start_time) * 1000
    time_list.append(elapsed_time_ms)
    rate = args.size_mb / (elapsed_time_ms / 1000)
    log(f'{rank} {i:03d}/{args.iters:d} sent {args.size_mb:d} MBs in {elapsed_time_ms:.1f}'
        f' ms: {rate:.2f} MB/second')

  min = np.min(time_list)
  median = np.median(time_list)

  log(f"min: {min:8.2f}, median: {median:8.2f}, mean: {np.mean(time_list):8.2f}")


def main():
  # run local benchmark in launcher and launch service
  if args.role == "launcher":
    run_launcher()
  elif args.role == "worker":
    run_worker()
  else:
    assert False, 'unknown role'


if __name__ == '__main__':
  main()
