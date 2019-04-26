# ncluster
By Yaroslav Bulatov, Andrew Shaw, Ben Mann

```
import ncluster
task = ncluster.make_task(instance_type='p2.xlarge')
task.upload('myscript.py')
task.run('python myscript.py > out')
task.download('out')
```

## Installation
Install pip, tmux, Python 3.6 (see below), then

```
pip install -r https://raw.githubusercontent.com/yaroslavvb/ncluster/master/requirements.txt
pip install -U ncluster
```

### Extra
An example of installing pip/tmux/python 3.6 on MacOS

1. Download Anaconda distribution following https://conda.io/docs/user-guide/install/index.html
2. Install tmux through homebrew: https://brew.sh/, then `brew install tmux`

Then

```
conda create -n new python=3.6 -y
conda activate new
```

Extra Deps:
```
brew install fswatch
```
