#!/bin/env python
# tests to make sure that logdir logic works
import inspect
import random
import sys
import threading

import ncluster


def test_two_jobs():
  run = ncluster.make_run('logdir_test')
  job1 = run.make_job('job1')
  task1 = job1.tasks[0]
  task1.run(f'echo hello > {task1.logdir}/message')
  job2 = run.make_job('job2')
  task2 = job2.tasks[0]
  assert task2.read(f'{task2.logdir}/message').strip() == 'hello'


def test_multiple_logdirs():
  logdir1 = ncluster.get_logdir_root() + '/test1'
  dummy_task = ncluster.make_task()
  dummy_task.run(f'rm -Rf {logdir1}')
  task1 = ncluster.make_task(run_name='test1')
  assert task1.logdir == logdir1

  logdir2 = ncluster.get_logdir_root() + '/test2'
  task2 = ncluster.make_task(run_name='test2')
  dummy_task.run(f'rm -Rf {logdir2}*')
  dummy_task.run(f'mkdir {logdir2}')
  assert task2.logdir == logdir2 + '.01'


def test_multiple_logdir_tasks():
  n = 10
  dummy_task = ncluster.make_task()
  logdir1 = ncluster.get_logdir_root() + '/test1'
  dummy_task.run(f'rm -Rf {logdir1}')
  job = ncluster.make_job(run_name='test1', num_tasks=n)

  obtained_logdirs = []

  import wrapt

  @wrapt.synchronized
  def query(i):
    obtained_logdirs.append(job.tasks[i].logdir)

  threads = [threading.Thread(target=query, args=(i,)) for i in range(n)]
  for thread in reversed(threads):
    thread.start()

  random.shuffle(threads)
  for thread in threads:
    thread.join()

  assert len(set(obtained_logdirs)) == 1
  assert obtained_logdirs[0] == logdir1


def run_all_tests(module):
  all_functions = inspect.getmembers(module, inspect.isfunction)
  for name, func in all_functions:
    if name.startswith('test'):
      print("Testing " + name)
      func()
  print(module.__name__ + " tests passed.")


def manual():
  run_all_tests(sys.modules[__name__])


if __name__ == '__main__':
  manual()
