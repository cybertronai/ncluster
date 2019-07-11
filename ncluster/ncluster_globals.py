"""Module that keeps global state of ncluster tasks, such as naming,
connection of tasks to runs.

run refers to string name
run_object refers to Run object corresponding to that name

"""
import os
import sys
from typing import Dict, Any, List

from . import aws_backend as backend
from . import util

task_launched = False  # keep track whether anything has been launched

task_counter = 0
job_counter = 0
run_counter = 0

run_dict: Dict[str, Any] = {}
task_run_dict: Dict["backend.Task", str] = {}
run_task_dict: Dict[str, List["backend.Task"]] = {}
run_logdir_dict: Dict[str, str] = {}

tasks_seen: List["backend.Task"] = []  # list of all tasks created

enforce_placement_group_val = False


def enforce_placement_group():
  """Enforces all tasks to be launched into placement group."""
  global enforce_placement_group_val
  enforce_placement_group_val = True


def unenforce_placement_group():
  """Enforces all tasks to be launched into placement group."""
  global enforce_placement_group_val
  enforce_placement_group_val = False


def is_enforced_placement_group():
  return enforce_placement_group_val


def auto_assign_task_name_if_needed(name, instance_type='', image_name='',
                                    tasks=1):
  global task_counter
  if name:
    return name

  main_script = os.path.abspath(sys.argv[0])
  script_id = util.alphanumeric_hash(
    f"{main_script}-{instance_type}-{image_name}-{tasks}")
  name = f"unnamedtask-{task_counter}-{script_id}"
  task_counter += 1
  return name


def auto_assign_job_name_if_needed(name):
  global job_counter
  if name:
    return name
  script_id = util.alphanumeric_hash(sys.argv[0])
  name = f"unnamedjob-{job_counter}-{script_id}"
  job_counter += 1
  return name


def auto_assign_run_name_if_needed(name):
  global run_counter
  if name:
    return name
  script_id = util.alphanumeric_hash(sys.argv[0])
  name = f"unnamedrun-{run_counter}-{script_id}"
  run_counter += 1
  return name


def register_task(task: Any, run_name: str):
  global task_run_dict, run_task_dict, tasks_seen
  assert task.name not in tasks_seen
  tasks_seen.append(task.name)
  task_run_dict[task] = run_name
  run_task_list = run_task_dict.get(run_name, [])
  run_task_list.append(task)

  # disable check because it's useless (instance creation fails with missing placement group before getting to register_task)
  # enforce uniformity -- either all tasks in a run are reused (assuming 1 job per run) or all tasks are created fresh
  # has_reuse = sum(task.instance_reuse for task in run_task_list)
  # has_fresh = sum(not task.instance_reuse for task in run_task_list)
  # if has_reuse + has_fresh != 1:
  #   tasks_to_kill = [task.name for task in run_task_list]
  #   print(f"Fatal: trying to reuse some instances while recreating others. Launching a group requires launching all "
  #         f"instances together. Kill following instances and try again: {','.join(tasks_to_kill)}")
  #   for task in run_task_list:
  #     print(f"{task.name}: {'reused' if task.instance_reuse else 'fresh'}")
  #   os.kill(os.getpid(), signal.SIGTERM)  # sys.exit() doesn't work inside thread


def register_run(run: "backend.Run", run_name: str) -> None:
  print(f"Registering run {run_name}")
  assert run_name not in run_dict
  assert run_name  # empty name reserved to mean no run
  run_dict[run_name] = run


def is_chief(task: "backend.Task", run_name: str):
  """Returns True if task is chief task in the corresponding run"""
  global run_task_dict
  if run_name not in run_task_dict:
    return True
  task_list = run_task_dict[run_name]
  assert task in task_list, f"Task {task.name} doesn't belong to run {run_name}"
  return task_list[0] == task


def get_chief(run_name: str):
  assert run_name in run_task_dict, f"Run {run_name} doesn't exist"
  tasks = run_task_dict[run_name]
  assert tasks, f"Run {run_name} had tasks {tasks}, expected non-empty list"
  return tasks[0]


def get_logdir(run_name: str):
  """Returns logdir for this run. It is the job of logdir creator to set logdir for this run"""

  if not run_name:
    return '/tmp'
  return run_logdir_dict.get(run_name, '')


def set_logdir(run_name, logdir):
  assert run_name not in run_logdir_dict, f"logdir for run {run_name} has already been set to {run_logdir_dict[run_name]}, trying to change it to {logdir} is illegal"
  run_logdir_dict[run_name] = logdir


def get_run_for_task(task: "backend.Task") -> str:
  """Gets run name associated with given Task"""
  return task_run_dict.get(task, '')


def get_run_object(run_name: str) -> "backend.Run":
  return run_dict.get(run_name, None)


def create_run_if_needed(run_name, run_creation_callback) -> "backend.Run":
  if run_name in run_dict:
    return run_dict[run_name]
  run = run_creation_callback(run_name)
  return run


_should_skip_setup = False


def set_should_skip_setup(val):
  global _should_skip_setup
  if val:
    util.log("skipping setup for all subsequent tasks/jobs")
  _should_skip_setup = val


def should_skip_setup():
  return _should_skip_setup
