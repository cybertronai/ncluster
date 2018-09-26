#!/usr/bin/env python


"""
TensorFlow distributed benchmark. Create sender/receiver tasks and add arrays from sender tasks to variable on receiver.

To run locally:
./tf_two_machines.py
Should see something like this

```
005/11 added 100 MBs in 78.9 ms: 1266.98 MB/second
006/11 added 100 MBs in 78.1 ms: 1280.07 MB/second
007/11 added 100 MBs in 78.1 ms: 1280.56 MB/second
008/11 added 100 MBs in 81.8 ms: 1222.76 MB/second
009/11 added 100 MBs in 79.5 ms: 1258.54 MB/second
010/11 added 100 MBs in 76.6 ms: 1305.64 MB/second
min:    76.59, median:    78.80, mean:    88.34
```

To interact with task 1 (the driver), do "tmux a -t 1"

To run on AWS
aws configure # or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_DEFAULT_REGION
./tf_two_machines.py --aws

Should see something like this with t3.large instances
```
089/100 added 100 MBs in 253.8 ms: 504.27 MB/second
090/100 added 100 MBs in 252.6 ms: 506.63 MB/second
091/100 added 100 MBs in 255.0 ms: 501.92 MB/second
```

Running c5.18xlarge machines with more iterations
007/11 sent 100 MBs in 135.4 ms: 738.47 MB/second
008/11 sent 100 MBs in 133.0 ms: 752.04 MB/second
009/11 sent 100 MBs in 133.8 ms: 747.48 MB/second
010/11 sent 100 MBs in 136.3 ms: 733.77 MB/second
min:   132.97, median:   134.98, mean:   137.27


Can use more shards
./tf_two_machines.py --aws --shards=8 --iters=1000
995/1000 sent 100 MBs in 87.0 ms: 1149.21 MB/second
996/1000 sent 100 MBs in 86.8 ms: 1152.11 MB/second
997/1000 sent 100 MBs in 89.8 ms: 1113.89 MB/second
998/1000 sent 100 MBs in 87.9 ms: 1137.37 MB/second
999/1000 sent 100 MBs in 88.0 ms: 1135.80 MB/second
min:    86.12, median:    88.48, mean:    89.51


To connect and interact with the job look for SSH instructions like this
   To connect to 0.tf_two_machines
   ssh -i /Users/yaroslav/.ncluster/ncluster2-yaroslav-316880547378-us-east-1.pem -o StrictHostKeyChecking=no ubuntu@18.234.30.222

ssh into the instance following these instructions, then run "tmux a"


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
                    default='Deep Learning AMI (Ubuntu) Version 14.0')
parser.add_argument('--name',
                    default='tf_two_machines')

# internal flags
parser.add_argument('--role', default='launcher', type=str)
parser.add_argument("--sender-ip", default='127.0.0.1')
parser.add_argument("--receiver-ip", default='127.0.0.1')
args = parser.parse_args()

cluster_spec = {'chief': [args.sender_ip + ':32300'],
                'receiver': [args.receiver_ip + ':32301']}


def _launch_server(role):
  os.environ['TF_CONFIG'] = json.dumps(
    {'cluster': cluster_spec,
     'task': {'type': role, 'index': 0}})
  config = tf.estimator.RunConfig()
  return tf.train.Server(config.cluster_spec,
                         job_name=config.task_type,
                         task_index=config.task_id)


def run_launcher():
  import ncluster
  if args.aws:
    ncluster.set_backend('aws')

  job = ncluster.make_job(args.name, num_tasks=2, image_name=args.image)
  job.upload(__file__)
  job.upload('util.py')

  sender, receiver = job.tasks
  # kill python just for when tmux session reuse is on
  if not ncluster.running_locally():
    sender._run_raw('killall python', ignore_errors=True)
    receiver._run_raw('killall python', ignore_errors=True)

  if ncluster.get_backend() == 'aws':
    # on AWS probably running in conda DLAMI, switch into TF-enabled env
    job.run('source activate tensorflow_p36')

  ip_config = f'--sender-ip={sender.ip} --receiver-ip={receiver.ip}'
  receiver.run(f'python {__file__} --role=receiver {ip_config}',
               non_blocking=True)
  sender.run(
    f'python {__file__} --role=sender {ip_config} --iters={args.iters} --size-mb={args.size_mb} --shards={args.shards}')
  print(sender.file_read('out'))


def run_receiver():
  server = _launch_server('receiver')
  time.sleep(365 * 24 * 3600)
  del server


def run_sender():
  param_size = 250 * 1000 * args.size_mb // args.shards  # 1MB is 250k integers
  log = util.FileLogger('out')
  grads_array = []
  with tf.device('/job:chief/task:0'):
    #    grads = tf.fill([param_size], 1.)
    for i in range(args.shards):
      grads = tf.Variable(tf.ones([param_size]))
      grads_array.append(grads)

  params_array = []
  add_op_array = []
  with tf.device('/job:receiver/task:0'):
    for i in range(args.shards):
      params = tf.Variable(tf.ones([param_size]))
      add_op = params.assign(grads_array[i]).op
      params_array.append(params)
      add_op_array.append(add_op)
    add_op = tf.group(*add_op_array)
    
  server = _launch_server('chief')
  sess = tf.Session(server.target)
  sess.run(tf.global_variables_initializer())
    # except Exception as e:
    #   # sometimes .run fails with .UnavailableError: OS Error
    #   log(f"initialization failed with {e}, retrying in 1 second")
    #   time.sleep(1)

  time_list = []
  for i in range(args.iters):
    start_time = time.perf_counter()
    sess.run(add_op)
    elapsed_time_ms = (time.perf_counter() - start_time) * 1000
    time_list.append(elapsed_time_ms)
    rate = args.size_mb / (elapsed_time_ms / 1000)
    log('%03d/%d sent %d MBs in %.1f ms: %.2f MB/second' % (
      i, args.iters, args.size_mb, elapsed_time_ms, rate))

  min = np.min(time_list)
  median = np.median(time_list)

  log(
    f"min: {min:8.2f}, median: {median:8.2f}, mean: {np.mean(time_list):8.2f}")


def main():
  # run local benchmark in launcher and launch service
  if args.role == "launcher":
    run_launcher()
  elif args.role == "sender":
    run_sender()
  elif args.role == "receiver":
    run_receiver()
  else:
    assert False, 'unknown role'


if __name__ == '__main__':
  main()
