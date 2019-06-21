# ncluster
By Yaroslav Bulatov, Andrew Shaw, Ben Mann

https://github.com/cybertronai/ncluster

## Installation
Install pip, tmux, Python 3.6 (see below), then

```
pip install -r https://raw.githubusercontent.com/yaroslavvb/ncluster/master/requirements.txt
pip install -U ncluster
export AWS_ACCESS_KEY_ID=AKIAIBATdf343
export AWS_SECRET_ACCESS_KEY=z7yKEP/RhO3Olk343aiP
export AWS_DEFAULT_REGION=us-east-1
```

## Python API

```
import ncluster
task = ncluster.make_task(instance_type='p2.xlarge')
task.upload('myscript.py')
task.run('python myscript.py > out')
task.download('out')
```



## Command-line tools

```
ncluster
ncluster ls
ncluster hosts
ncluster ls
ncluster ls <substring>
ncluster ssh # connects to latest instance
ncluster ssh <substring>  # connects to latest instance containing <substring>
ncluster ssh \'<exact match>\'
ncluster mosh <substring> 
ncluster kill <substring>    # terminates matching instances
ncluster kill \'<exact match>\'
ncluster stop <substring>    # stops matching instances
ncluster start <substring>   # starts matching stopped instances
ncluster nano       # starts a tiny instance
ncluster keys   # information on enabling SSH access for your team-members

ncluster ssh_    # like ssh but works on dumb terminals
ncluster ls     
ncluster cat <fn>
ncluster cmd "some command to run remotely on AWS"

ncluster efs   # gives EFS info such as the mount command

nsync -n gpubox
nsync -n gpubox -t transformer-xl

nsync -t {target directory} -n {substring}

nsync -n gpubox # syncs . to ~ on gpubox
nsync -t transformer-xl -n 4gpubox  # syncs . to ~/transformer-xl on 4gpubox


{substring} selects the most recently launched instances whose name contains the substring. Empty string is a valid substring. Skipping -t will sync to ~ on remote machine. Sync seems to be 1 way (from local -> remote)
```

## Docs
- Some out-of-date docs with more info [docs](https://docs.google.com/document/d/178ITRCAkboHoOEZFnz9XvOsc8lXik6Acz_DS_V1u8hY/edit?usp=sharing)

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
