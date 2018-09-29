#!/usr/bin/env python
import ncluster

# allocate default machine type and default image
task = ncluster.make_task()
output = task.run('ifconfig')
print(f"Task ifconfig returned {output}")
