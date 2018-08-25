import ncluster

# allocate default machine type and default image
ncluster.set_backend('local')

task = ncluster.make_task(name='test')
print(task.file_read('/etc/hosts'))

# for faster startup, switch to Amazon Linux image, ie
# make_task(image_name='amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')
