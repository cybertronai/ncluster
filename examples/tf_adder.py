#!/usr/bin/env python


"""
TensorFlow distributed benchmark. Create sender/receiver tasks and add arrays from sender tasks to variable on receiver.

To run locally:
unset NCLUSTER_BACKEND
./tf_adder.py
tmux a -t 0

Should see something like this
```
088/100 added 128 MBs in 123.2 ms: 1038.89 MB/second
089/100 added 128 MBs in 114.9 ms: 1114.36 MB/second
090/100 added 128 MBs in 113.4 ms: 1128.61 MB/second
091/100 added 128 MBs in 113.4 ms: 1128.60 MB/second
092/100 added 128 MBs in 118.3 ms: 1082.34 MB/second
093/100 added 128 MBs in 111.7 ms: 1145.50 MB/second
094/100 added 128 MBs in 113.4 ms: 1128.63 MB/second
095/100 added 128 MBs in 113.6 ms: 1126.71 MB/second
096/100 added 128 MBs in 115.5 ms: 1108.68 MB/second
097/100 added 128 MBs in 114.3 ms: 1119.58 MB/second
098/100 added 128 MBs in 114.5 ms: 1117.51 MB/second
```


To run on AWS
export NCLUSTER_BACKEND=aws
export NCLUSTER_IMAGE="Deep Learning AMI (Amazon Linux) Version 13.0"
./tf_adder.py
nconnect 0.tf_adder

Should see something like this with t3.large instances
```
087/100 added 128 MBs in 254.1 ms: 503.67 MB/second
088/100 added 128 MBs in 252.8 ms: 506.42 MB/second
089/100 added 128 MBs in 253.8 ms: 504.27 MB/second
090/100 added 128 MBs in 252.6 ms: 506.63 MB/second
091/100 added 128 MBs in 255.0 ms: 501.92 MB/second
092/100 added 128 MBs in 253.3 ms: 505.30 MB/second
093/100 added 128 MBs in 254.0 ms: 503.99 MB/second
094/100 added 128 MBs in 253.9 ms: 504.04 MB/second
095/100 added 128 MBs in 253.5 ms: 504.99 MB/second
096/100 added 128 MBs in 253.8 ms: 504.35 MB/second
```

"""

import argparse
import json
import os
import tensorflow as tf
import time

parser = argparse.ArgumentParser()
parser.add_argument('--role', default='launcher', type=str)
parser.add_argument("--iters", default=100, help="Maximum number of additions")
parser.add_argument("--data-mb", default=128, help="size of vector in MBs")
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

  job = ncluster.make_job('tf_adder', num_tasks=2)
  job.upload(__file__)
  sender, receiver = job.tasks
  if ncluster.get_backend() == 'aws':
    # on AWS probably are running in DLAMI, switch into TF-enabled env
    job.run('source activate tensorflow_p36')

  ip_config = f'--sender-ip={sender.ip} --receiver-ip={receiver.ip}'
  job.tasks[1].run(f'python tf_adder.py --role=receiver {ip_config}', async=True)
  job.tasks[0].run(f'python tf_adder.py --role=sender {ip_config}')


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

  for i in range(args.iters):
    start_time = time.time()
    sess.run(add_op)
    elapsed_time = time.time() - start_time
    rate = args.data_mb / elapsed_time
    print('%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (i,
                                                               args.iters,
                                                               args.data_mb,
                                                               elapsed_time * 1000,
                                                               rate))


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
