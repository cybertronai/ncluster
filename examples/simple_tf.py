#!/bin/env python
import sys

if not sys.argv[1:]:
  import ncluster
  task = ncluster.make_task(instance_type='t3.micro')
  task.upload(__file__)
  task.run('pip install tensorflow')
  task.run(f'python {__file__} worker')
elif sys.argv[1] == 'worker':
  import tensorflow as tf
  import os
  sess = tf.Session()
  ones = tf.ones((1000,1000))
  result = sess.run(tf.matmul(ones, ones))
  print(f"matmul gave {result.sum()}")
  os.system('sudo shutdown -h -P 10')  # shut down instead in 10 mins

