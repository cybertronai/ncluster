#!/usr/bin/env python
import argparse
import json
import os
import tensorflow as tf
import time

from tensorflow.contrib.learn.python.learn.estimators.run_config import ClusterConfig

parser = argparse.ArgumentParser()
parser.add_argument('--role', default='launcher', type=str)
parser.add_argument("--iters", default=10, help="Maximum number of additions")
parser.add_argument("--data_mb", default=128, help="size of vector in MBs")
args = parser.parse_args()

host = "127.0.0.1"


def session_config():
  optimizer_options = tf.OptimizerOptions(opt_level=tf.OptimizerOptions.L0)
  config = tf.ConfigProto(
    graph_options=tf.GraphOptions(optimizer_options=optimizer_options))
  config.log_device_placement = False
  config.allow_soft_placement = False
  return config


cluster_spec = {'chief': ['localhost:32301'],
                'receiver': ['localhost:32302']}


def launcher():
  import ncluster

  job = ncluster.make_job('tf_adder', num_tasks=2)
  job.upload(__file__)
  job.tasks[0].run('python tf_adder.py --role=receiver', async=True)
  job.tasks[1].run('python tf_adder.py --role=sender')


def receiver():
  os.environ['TF_CONFIG'] = json.dumps(
    {'cluster': cluster_spec,
     'task': {'type': 'receiver', 'index': 0}})

  config = ClusterConfig()
  assert config.task_type == 'receiver'
  server = tf.train.Server(config.cluster_spec,
                           config=session_config(),
                           job_name=config.task_type,
                           task_index=config.task_id)
  time.sleep(365 * 24 * 3600)


def sender():
  """Connect to master and run simple TF->Python transfer benchmark."""

  os.environ['TF_CONFIG'] = json.dumps(
    {'cluster': cluster_spec,
     'task': {'type': 'chief', 'index': 0}})
  config = tf.estimator.RunConfig()

  param_size = 250 * 1000 * args.data_mb  # 1MB is 250k integers
  with tf.device('/job:chief/task:0'):
    grads = tf.fill([param_size], 1.)

  with tf.device('/job:receiver/task:0'):
    params = tf.Variable(tf.ones([param_size]))
    add_op = params.assign_add(grads).op

  server = tf.train.Server(config.cluster_spec,
                           config=session_config(),
                           job_name=config.task_type,
                           task_index=config.task_id)

  sess = tf.Session(server.target, config=session_config())
  sess.run(tf.global_variables_initializer())

  start_time = time.time()
  for i in range(10):
    print('adding')
    sess.run(add_op)

  elapsed_time = time.time() - start_time
  rate = float(args.iters) * args.data_mb / elapsed_time
  print(f"%.2f MB/second" % rate)


def main():
  # run local benchmark in launcher and launch service
  if args.role == "sender":
    sender()
  elif args.role == "receiver":
    receiver()
  elif args.role == "launcher":
    launcher()
  else:
    assert False, 'unknown role'


if __name__ == '__main__':
  main()
