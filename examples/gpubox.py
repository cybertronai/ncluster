#!/usr/bin/env python
#
# Launch a single GPU instance with jupyter notebook

import argparse
import os
import ncluster


parser = argparse.ArgumentParser()
parser.add_argument('--name', type=str, default='gpubox',
                    help="instance name")
parser.add_argument('--image-name', type=str,
                    default='', # 'Deep Learning AMI (Ubuntu) Version 12.0',
                    help="name of AMI to use ")
parser.add_argument('--instance-type', type=str, default='g3.4xlarge',
                    help="type of instance")
parser.add_argument('--password',
                    default='DefaultNotebookPasswordPleaseChange',
                    help='password to use for jupyter notebook')
args = parser.parse_args()
module_path = os.path.dirname(os.path.abspath(__file__))


def main():
  task = ncluster.make_task(name=args.name,
                            instance_type=args.instance_type,
                            image_name=args.image_name)
  
  # upload notebook config with provided password
  jupyter_config_fn = _create_jupyter_config(args.password)
  remote_config_fn = '/home/ubuntu/.jupyter/jupyter_notebook_config.py'
  task.upload(jupyter_config_fn, remote_config_fn)

  # upload sample notebook and start Jupyter server
  task.run('mkdir -p /efs/notebooks')
  task.upload(f'{module_path}/gpubox_sample.ipynb',
              '/efs/notebooks/gpubox_sample.ipynb',
              dont_overwrite=True)
  task.run('cd /efs/notebooks')
  task.run('jupyter notebook', async=True)
  print(f'Jupyter notebook will be at http://{task.public_ip}:8888')


def _create_jupyter_config(password):
  from notebook.auth import passwd
  sha = passwd(args.password)
  local_config_fn = f'{module_path}/gpubox_jupyter_notebook_config.py'
  temp_config_fn = '/tmp/'+os.path.basename(local_config_fn)
  os.system(f'cp {local_config_fn} {temp_config_fn}')
  _replace_lines(temp_config_fn, 'c.NotebookApp.password',
                 f"c.NotebookApp.password = '{sha}'")
  return temp_config_fn

def _replace_lines(fn, startswith, new_line):
  """Replace lines starting with starts_with in fn with new_line."""
  new_lines = []
  for line in open(fn):
    if line.startswith(startswith):
      new_lines.append(new_line)
    else:
      new_lines.append(line)
  with open(fn, 'w') as f:
    f.write('\n'.join(new_lines))
  

if __name__=='__main__':
  main()
