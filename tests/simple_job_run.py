import ncluster
import time

# TODO: pdb_handler should run by default
#   ncluster.aws_util.install_pdb_handler()

def main():
  ncluster.set_backend('local')

  job = ncluster.make_job(num_tasks=2)

  start_time = time.time()
  job.run('sleep 1')
  print(f"waited for {time.time()-start_time} seconds")

if __name__ == '__main__':
  main()
