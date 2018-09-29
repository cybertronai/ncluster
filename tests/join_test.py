import ncluster
import pytest

def test():
  task = ncluster.make_task(image_name=ncluster.aws_backend.GENERIC_SMALL_IMAGE)
  task.run("mkdir /illegal", non_blocking=True)
  task.join(ignore_errors=True)  # this succeed/print error message

  task.run("mkdir /illegal", non_blocking=True)
  with pytest.raises(RuntimeError):
    task.join()  # this should fail

if __name__ == '__main__':
  test()
