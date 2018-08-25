import ncluster

# allocate default machine type and default image
task = ncluster.make_task()
print(task.file_read('/proc/cpuinfo'))


# for faster startup, switch to Amazon Linux image, ie
# make_task(image_name='amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')
