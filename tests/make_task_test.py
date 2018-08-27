import ncluster

if __name__ == '__main__':
  ncluster.aws_util.install_pdb_handler()
  #  task = ncluster.make_task(image_name='amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')
  task = ncluster.make_task(name='ncluster-1535154315336620',
                            image_name='amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')

  task.join()
  print(task.file_read('/proc/cpuinfo'))
