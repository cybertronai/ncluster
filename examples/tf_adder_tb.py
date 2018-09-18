#!/usr/bin/env python


"""
TensorFlow distributed benchmark + TensorBoard. Create sender/receiver tasks and add arrays from sender tasks to
variable on receiver.

To run locally:
./tf_adder_tb.py

Should see something like this
```
...
Benchmark done, tensorboard at http://127.0.0.1:6006
```


To run on AWS
aws configure # or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_DEFAULT_REGION

./tf_adder_tb.py --aws

After a minute should see something like this

...
Benchmark done, tensorboard at http://35.173.134.87:6006
"""

import argparse
import json
import os
import tensorflow as tf
import time

parser = argparse.ArgumentParser()
parser.add_argument('--role', default='launcher', type=str)
parser.add_argument("--iters", default=20, help="Maximum number of additions")
parser.add_argument("--data-mb", default=128, help="size of vector in MBs")
parser.add_argument("--sender-ip", default='127.0.0.1')
parser.add_argument("--receiver-ip", default='127.0.0.1')
parser.add_argument("--logdir", help='logging directory')
parser.add_argument("--aws", action='store_true')
parser.add_argument('--image', default='Deep Learning AMI (Amazon Linux) Version 13.0')
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
  job = ncluster.make_job('tf_adder_tb', num_tasks=2, image_name=args.image)
  job.upload(__file__)
  this_file = os.path.basename(__file__)

  sender, receiver = job.tasks
  if ncluster.get_backend() == 'aws':
    # on AWS probably are running in DLAMI, switch into TF-enabled env
    job.run('source activate tensorflow_p36')

  ip_config = f'--sender-ip={sender.ip} --receiver-ip={receiver.ip}'
  job.tasks[1].run(f'python {this_file} --role=receiver {ip_config}', non_blocking=True)
  job.tasks[0].run(f'python {this_file} --role=sender --logdir={job.logdir} {ip_config}')
  job.tasks[0].run(f'tensorboard --logdir={job.logdir}/..', non_blocking=True)
  print(f"Benchmark done, tensorboard at http://{job.tasks[0].public_ip}:6006")


def run_receiver():
  server = _launch_server('receiver')
  time.sleep(365 * 24 * 3600)
  del server


def run_sender():
  summary_writer = tf.summary.FileWriter(args.logdir)

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
    print('%03d/%d added %d MBs in %.1f ms: %.2f MB/second' % (i, args.iters, args.data_mb, elapsed_time * 1000, rate))
    summary = tf.Summary()
    summary.value.add(tag='time_ms', simple_value=elapsed_time * 1000)
    summary_writer.add_summary(summary, i)

  summary_writer.close()


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
