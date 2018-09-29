import ncluster
import time

def main():
  ncluster.set_backend('aws')
  
  start_time = time.time()
  job = ncluster.make_job(num_tasks=16)
  print(f"waited for startup for {time.time()-start_time} seconds")

  start_time = time.time()
  job.run('sleep 10')
  print(f"waited for exec for {time.time()-start_time} seconds")

if __name__ == '__main__':
  main()
