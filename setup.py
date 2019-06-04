from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(scripts=['ncluster/aws_create_resources.py',  # also used as module
               'ncluster/aws_delete_resources.py',
               'tools/nsync',
               'tools/ncluster'],
      install_requires=requirements,
)
