#!/usr/bin/env python


"""
TensorFlow distributed benchmark. Create sender/receiver tasks and add arrays from sender tasks to variable on receiver.

To run locally:
./tf_two_machines.py
tmux a -t 0

Should see something like this
```
089/100 added 100 MBs in 114.9 ms: 1114.36 MB/second
090/100 added 100 MBs in 113.4 ms: 1128.61 MB/second
091/100 added 100 MBs in 113.4 ms: 1128.60 MB/second
```


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
1987/2000 added 100 MBs in 109.2 ms: 916.15 MB/second
1988/2000 added 100 MBs in 109.5 ms: 913.07 MB/second
1989/2000 added 100 MBs in 111.1 ms: 900.48 MB/second
1990/2000 added 100 MBs in 109.4 ms: 913.73 MB/second
1991/2000 added 100 MBs in 122.4 ms: 816.97 MB/second
1992/2000 added 100 MBs in 123.5 ms: 809.73 MB/second
1993/2000 added 100 MBs in 122.2 ms: 818.63 MB/second
1994/2000 added 100 MBs in 111.2 ms: 899.45 MB/second
1995/2000 added 100 MBs in 110.1 ms: 908.59 MB/second
1996/2000 added 100 MBs in 109.9 ms: 910.03 MB/second

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

parser = argparse.ArgumentParser()
parser.add_argument("--aws", action="store_true", help="enable to run on AWS")
parser.add_argument("--iters", default=11, type=int, help="Maximum number of additions")
parser.add_argument("--data-mb", default=100, type=int, help="size of vector in MBs")
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
  
  sender, receiver = job.tasks
  if ncluster.get_backend() == 'aws':
    # on AWS probably running in conda DLAMI, switch into TF-enabled env
    job.run('source activate tensorflow_p36')

  ip_config = f'--sender-ip={sender.ip} --receiver-ip={receiver.ip}'
  receiver.run(f'python {__file__} --role=receiver {ip_config}',
               non_blocking=True)
  sender.run(f'python {__file__} --role=sender {ip_config} --iters={args.iters}')
  print(sender.file_read('out'))


def run_receiver():
  server = _launch_server('receiver')
  time.sleep(365 * 24 * 3600)
  del server


def run_sender():
  param_size = 250 * 1000 * args.data_mb  # 1MB is 250k integers
  with tf.device('/job:chief/task:0'):
    grads = tf.fill([param_size], 1.)

  with tf.device('/job:receiver/task:0'):
    params = tf.Variable(tf.ones([param_size]))
    add_op = params.assign_add(grads).op

  server = _launch_server('chief')
  sess = tf.Session(server.target)

  sess.run(tf.global_variables_initializer())

  time_list = []
  result = ''
  for i in range(args.iters):
    start_time = time.perf_counter()
    sess.run(add_op)
    elapsed_time_ms = (time.perf_counter() - start_time)*1000
    time_list.append(elapsed_time_ms)
    rate = args.data_mb / (elapsed_time_ms/1000)
    line = '%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (i, args.iters, args.data_mb, elapsed_time_ms, rate)
    print(line)
    result = result + line + '\n'

  min = np.min(time_list)
  median = np.median(time_list)
  
  result += f"min: {min:8.2f}, median: {median:8.2f}, mean: {np.mean(time_list):8.2f}"
  with open('out', 'w') as f:
    f.write(result+'\n')


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
