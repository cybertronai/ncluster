
import argparse
import os
import random
import string
import sys

import wandb

# in test environments disable pdb intercept
os.environ['NCLUSTER_DISABLE_PDB_HANDLER'] = '1'

parser = argparse.ArgumentParser()
parser.add_argument('--name', type=str, default='integration_test', help="job name")
parser.add_argument('--instance_type', type=str, default="c5.large")
parser.add_argument('--num_tasks', type=int, default=2)
parser.add_argument('--image_name', type=str, default='Deep Learning AMI (Ubuntu) Version 23.0')
parser.add_argument('--spot', action='store_true',
                    help='use spot instead of regular instances')

parser.add_argument('--nproc_per_node', type=int, default=1)
parser.add_argument('--conda_env', type=str, default='pytorch_p36')

parser.add_argument('--skip_setup', action='store_true')
parser.add_argument('--local_rank', default=0, type=int)


parser.add_argument('--role', type=str, default='launcher',
                    help='internal flag, launcher or worker')
args = parser.parse_args()


def random_id(k=5):
    """Random id to use for AWS identifiers."""
    #  https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))


def launcher():
    # run this test out of root directory of ncluster to capture .git and requirements.txt
    script_fn = 'tests/integration_test.py'
    
    import ncluster
    job = ncluster.make_job(**vars(args))
    job.rsync('.')
    job.run('pip install -r requirements.txt')
    task0 = job.tasks[0]
    
    task0.run(f'python {script_fn} --role=worker --name={args.name}-{random_id()} --local_rank=0', stream_output=True)


def main():
    if args.role == "launcher":
        launcher()
    elif args.role == "worker":
        # rank = int(os.environ.get('OMPI_COMM_WORLD_LOCAL_RANK', 0))  # ompi way
        # rank = int(os.environ.get('RANK', '0'))  # pytorch way
        rank = args.local_rank  # cmd args way

        if rank != 0:
            os.environ['WANDB_MODE'] = 'dryrun'  # all wandb.log are no-op
        wandb.init(project='ncluster', name=args.name, entity='circleci')
        print(f"{os.uname()[1]} {rank} {' '.join(sys.argv)}")
        sys.stdout.flush()


if __name__ == '__main__':
    main()
