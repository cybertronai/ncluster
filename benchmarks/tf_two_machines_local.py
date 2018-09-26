#!/usr/bin/env python
"""
Runs distributed benchmark on a single machine remotely

Adding 100MB buffers

# 1 shard: 88ms
# 4 shards: 56ms
# 8 shards: 51ms
# 16 shards: 55ms

# increase size 8x
python tf_two_machines_local.py --shards=8 --iters=100 --size-mb=800 --aws
# 416ms

Bottom line: 1.6-1.9 GB/second when running locally
Going 1->4 shards saves 30%, 4->8 shards another 5%

i3.metal 30% slower than c5.18xlarge

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
                    default='tf_two_machines_local')

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
  ncluster.util.assert_script_in_current_directory()
  
  if args.aws:
    ncluster.set_backend('aws')

  # use 4GB instance, 0.5GB not enough
  worker = ncluster.make_task(args.name, image_name=args.image,
                              instance_type='t3.medium')
  worker.upload(__file__)
  worker.upload('util.py')

  # kill python just for when tmux session reuse is on
  if not ncluster.running_locally():
    # on AWS probably running in conda DLAMI, switch into TF-enabled env
    worker._run_raw('killall python', ignore_errors=True)
    worker.run('source activate tensorflow_p36')

  ip_config = f'--sender-ip={worker.ip} --receiver-ip={worker.ip}'
  worker.run(f'python {__file__} --role=receiver {ip_config}',
               non_blocking=True)
  worker.switch_window(1)  # run in new tmux window
  if not ncluster.running_locally():
    worker.run('source activate tensorflow_p36')
  worker.run(
    f'python {__file__} --role=sender {ip_config} --iters={args.iters} --size-mb={args.size_mb} --shards={args.shards}')
  print(worker.file_read('out'))


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
