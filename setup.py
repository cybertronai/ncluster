from setuptools import setup

requirements = []
for line in open('requirements.txt'):
    req = line.split('#', 1)[0]  # strip comments
    requirements.append(req.strip())
    
setup(scripts=['ncluster/aws_create_resources.py',  # also used as module
               'ncluster/aws_delete_resources.py',
               'tools/nsync',
               'tools/ncluster'],
      install_requires=requirements,
)
