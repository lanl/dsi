import atexit
import fileinput
import re
from setuptools import setup
from setuptools.command.install import install
import subprocess

def get_cmd_output(cmd: list) -> str:
    proc = subprocess.run(cmd, capture_output=True, shell=True)
    if proc.stderr != b"":
        raise Exception(proc.stderr)
    return proc.stdout.strip().decode("utf-8")

def insert_git_commit(replacement_target, old, new):
    with fileinput.FileInput(replacement_target, inplace=True, backup='.bak') as fh:
        for line in fh:
            op = line.replace(old,new)
            print(op, end='')

# Get the current git hash
sha = get_cmd_output(cmd=['git rev-parse HEAD'])
# Get the root of the git project
#git_root = get_cmd_output(cmd=['git rev-parse --show-toplevel'])

# String replace git_sha_commit placeholder in Plugin implementations
#replacement_target='/'.join([git_root,'dsi/plugins/metadata.py'])
replacement_target='dsi/plugins/metadata.py'
old = "a-box-of-one-dozen-starving-crazed-weasels"
new = sha

def file_insert():
    subprocess.check_call("ls -altrh {}".format(replacement_target))
    subprocess.check_call("sed -i s/a-box-of-one-dozen-starving-crazed-weasels/20b7f740224b8bb6f6c33493856573c2bd0bc98c/g {}".format(replacement_target))

def file_remove():
    insert_git_commit(replacement_target, new, old)

class SetupWrapper(install):
    def __init__(self,*args, **kwargs):
        file_insert()
        install.run(self)
        file_remove()

setup(cmdclass={'install':SetupWrapper})

