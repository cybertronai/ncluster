# ncluster
By Yaroslav Bulatov, Andrew Shaw, Ben Mann
https://github.com/cybertronai/ncluster

Ncluster provides Python API to do the following things:
- Allocate AWS machine
- Upload file to machine
- Run command on machine
- Download file from machine

IE

```
import ncluster
task = ncluster.make_task(instance_type='p2.xlarge')
task.upload('myscript.py')
task.run('python myscript.py > out')
task.download('out')
```

Necessary AWS infrastructure is created on demand using defaults optimal for fast prototyping. IE, your machines are preconfigured for passwordless SSH, can access each other over all interfaces, and have a persistent file system mounted under /ncluster. Commands are executed in a remote tmux session so you can take over the environment at any time and continue from your terminal.  


## Installation
Install pip, tmux, Python 3.6 (see below), and [write down](https://docs.google.com/document/d/1Z8lCZVWXs7XORbiNmBAsBDtouV3KwrtH8-UL5M-zHus/edit) your AWS security keys, then

```
pip install -r https://raw.githubusercontent.com/yaroslavvb/ncluster/master/requirements.txt
pip install -U ncluster
export AWS_ACCESS_KEY_ID=AKIAIBATdf343
export AWS_SECRET_ACCESS_KEY=z7yKEP/RhO3Olk343aiP
export AWS_DEFAULT_REGION=us-east-1
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

nsync -m gpubox
nsync -m gpubox -d transformer-xl

nsync -d {target directory} -m {machine name substring}

nsync -m gpubox # syncs . to ~ on gpubox
nsync -d transformer-xl -m 4gpubox  # syncs . to ~/transformer-xl on 4gpubox


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
