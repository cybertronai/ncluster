from setuptools import setup
 # aws_{create/delete}_resources is also lib, so have to keep it in nclustet
setup(scripts=['ncluster/aws_create_resources.py',
               'ncluster/aws_delete_resources.py',
               'tools/nsync',
               'tools/ncluster'
])
