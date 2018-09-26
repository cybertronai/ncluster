import ncluster

def test():
  run = ncluster.make_run('testing')
  job1 = run.make_job('job1')
  task1 = job1.tasks[0]
  task1.run(f'echo task1sayshello > {task1.logdir}/message')
  job2 = run.make_job('job2')
  task2 = job2.tasks[0]
  print(task2.run_with_output(f'cat {task2.logdir}/message'))
  

if __name__ == '__main__':
  test()
