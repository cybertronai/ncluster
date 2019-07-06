from setuptools import setup
import re

requirements = []
for line in open('requirements.txt'):
  req = line.split('#', 1)[0]  # strip comments
  requirements.append(req.strip())

# follow https://stackoverflow.com/a/7071358/419116
VERSIONFILE = "ncluster/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
  verstr = mo.group(1)
else:
  raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

setup(scripts=['ncluster/ncluster_cloud_setup.py',  # also used as module
               'ncluster/ncluster_cloud_wipe.py',
               'tools/nsync',
               'tools/ncluster'],
      install_requires=requirements,
      version=verstr,
      )
