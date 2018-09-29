#!/usr/bin/env python
#
# Run locally:
# ./pytorch_p2p.py
# 000/10 added 100 MBs in 35.0 ms: 2854.88 MB/second
# 001/10 added 100 MBs in 25.1 ms: 3979.37 MB/second
# 002/10 added 100 MBs in 25.4 ms: 3935.73 MB/second
# 003/10 added 100 MBs in 24.7 ms: 4040.93 MB/second
# 004/10 added 100 MBs in 24.4 ms: 4097.57 MB/second
# min:    21.58, median:    24.97, mean:    25.61

# To run on AWS:
# export NCLUSTER_IMAGE='Deep Learning AMI (Ubuntu) Version 14.0'
# export NCLUSTER_INSTANCE=c5.18xlarge
# python pytorch_p2p.py --aws
# 990/1000 added 100 MBs in 83.7 ms: 1194.35 MB/second
# 991/1000 added 100 MBs in 83.4 ms: 1198.78 MB/second
# 992/1000 added 100 MBs in 83.4 ms: 1198.73 MB/second
# 993/1000 added 100 MBs in 83.3 ms: 1201.20 MB/second
# 994/1000 added 100 MBs in 83.1 ms: 1203.84 MB/second
# 995/1000 added 100 MBs in 83.1 ms: 1203.04 MB/second
# 996/1000 added 100 MBs in 83.5 ms: 1197.38 MB/second
# 997/1000 added 100 MBs in 82.4 ms: 1213.99 MB/second
# 998/1000 added 100 MBs in 84.2 ms: 1187.69 MB/second
# 999/1000 added 100 MBs in 83.0 ms: 1204.13 MB/second
# min:    80.52, median:    83.25, mean:    83.29

import os
import sys
import time
import argparse
import util

parser = argparse.ArgumentParser(description='launch')

# launcher flags
parser.add_argument('--name', type=str, default='pytorch_two_machines',
                     help="name of the current run")
parser.add_argument('--size-mb', type=int, default=100,
                    help='size of data to send')
parser.add_argument('--iters', type=int, default=10,
                    help='how many iterations')
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")


# mpi flags
parser.add_argument('--role', type=str, default='launcher',
                    help='internal flag, launcher or worker')
parser.add_argument('--rank', type=int, default=0,
                    help='mpi rank')
parser.add_argument('--size', type=int, default=0,
                    help='size of mpi world')
parser.add_argument('--master-addr', type=str, default='127.0.0.1',
                    help='address of master node')
parser.add_argument('--master-port', type=int, default=6006,
                    help='port of master node')
args = parser.parse_args()

def worker():
  """ Initialize the distributed environment. """

  import torch
  import torch.distributed as dist
  from torch.multiprocessing import Process
  import numpy as np

  print("Initializing distributed pytorch")
  os.environ['MASTER_ADDR'] = str(args.master_addr)
  os.environ['MASTER_PORT'] = str(args.master_port)
  # Use TCP backend. Gloo needs nightly, where it currently fails with
  #     dist.init_process_group('gloo', rank=args.rank,
  #   AttributeError: module 'torch.distributed' has no attribute 'init_process_group'
  dist.init_process_group('tcp', rank=args.rank,
                          world_size=args.size)

  tensor = torch.ones(args.size_mb*250*1000)*(args.rank+1)
  time_list = []
  outfile = 'out' if args.rank == 0 else '/dev/null'
  log = util.FileLogger(outfile)
  for i in range(args.iters):
    # print('before: rank ', args.rank, ' has data ', tensor[0])

    start_time = time.perf_counter()
    if args.rank == 0:
      dist.send(tensor=tensor, dst=1)
    else:
      dist.recv(tensor=tensor, src=0)
      
    elapsed_time_ms = (time.perf_counter() - start_time)*1000
    time_list.append(elapsed_time_ms)
    # print('after: rank ', args.rank, ' has data ', tensor[0])
    rate = args.size_mb/(elapsed_time_ms/1000)

    log('%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (i, args.iters, args.size_mb, elapsed_time_ms, rate))

  min = np.min(time_list)
  median = np.median(time_list)
  log(f"min: {min:8.2f}, median: {median:8.2f}, mean: {np.mean(time_list):8.2f}")


def launcher():
  import ncluster
  
  if args.aws:
    ncluster.set_backend('aws')

  job = ncluster.make_job(args.name, num_tasks=2)
  job.upload(__file__)
  job.upload('util.py')

  if args.aws:
    job.run('source activate pytorch_p36')
  else:
    job.run('source deactivate')
    job.run('source activate ncluster-test3')

  script_name = os.path.basename(__file__)
  common_args = f'--size=2 --master-addr={job.tasks[0].ip} --iters={args.iters} --size-mb={args.size_mb}'
  job.tasks[0].run(f'python {script_name} --role=worker --rank=0 '+common_args,
                   non_blocking=True)
  job.tasks[1].run(f'python {script_name} --role=worker --rank=1 '+common_args,
                   non_blocking=True)

  job.tasks[0].join()
  print(job.tasks[0].read('out'))
    

def main():
  if args.role == "launcher":
    launcher()
  elif args.role == "worker":
    worker()
  else:
    assert False, "Unknown role "+FLAGS.role

  
if __name__ == "__main__":
  main()
