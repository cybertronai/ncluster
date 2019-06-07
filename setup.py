from setuptools import setup

requirements = []
for line in open('requirements.txt'):
    req = line.split('#', 1)[0]  # strip comments
    requirements.append(req.strip())
    
setup(scripts=['ncluster/ncluster_cloud_setup.py',  # also used as module
               'ncluster/ncluster_cloud_wipe.py',
               'tools/nsync',
               'tools/ncluster'],
      install_requires=requirements,
)
