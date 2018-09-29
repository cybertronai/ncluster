import ncluster

def test():
  run = ncluster.make_run('run_test')
  job1 = run.make_job('job1')
  task1 = job1.tasks[0]
  assert task1.name == '0.job1.run_test'
  task1.run(f'echo task1sayshello > {task1.logdir}/message')
  job2 = run.make_job('job2')
  task2 = job2.tasks[0]
  assert task2.name == '0.job2.run_test'
  assert task2.read(f'{task2.logdir}/message').strip() == 'task1sayshello'
  

if __name__ == '__main__':
  test()
