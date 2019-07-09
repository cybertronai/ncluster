import ncluster
import ncluster.util as util

# Test for a fix to exception with too many concurrent connections ("paramiko.ssh_exception.ChannelException: (1, 'Administratively prohibited'))



def test():
  task = ncluster.make_task('test2')
  for i in range(20):
    task.run('ls', stream_output=True)
  

if __name__ == '__main__':
  test()
