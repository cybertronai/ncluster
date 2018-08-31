#!/usr/bin/env python
import ncluster

# allocate default machine type and default image
task = ncluster.make_task()
stdout, stderr = task.run_with_output('ifconfig')
print(f"Task ifconfig returned {stdout, stderr}")

# for faster startup, switch to Amazon Linux image, ie
# make_task(image_name='amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2')
