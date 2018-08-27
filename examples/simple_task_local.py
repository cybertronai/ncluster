import ncluster

ncluster.set_backend('local')
task = ncluster.make_task(name='test')
print(task.file_read('/etc/hosts'))
