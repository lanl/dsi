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

def insert_git_commit(replacement_target, sha):
    with fileinput.FileInput(replacement_target, inplace=True) as fh:
        for line in fh:
            op = re.sub(r"git_commit_sha=.*",
                        r"git_commit_sha='{}'".format(sha), line)
            print(op, end='')

# Get the current git hash
sha = get_cmd_output(cmd=['git rev-parse HEAD'])
# Get the root of the git project
git_root = get_cmd_output(cmd=['git rev-parse --show-toplevel'])

# String replace git_sha_commit placeholder for Plugin and Driver implementations
metadata='/'.join([git_root,'dsi/plugins/metadata.py'])
filesystem='/'.join([git_root,'dsi/drivers/filesystem.py'])

class SetupWrapper(install):
    def run(self):
        insert_git_commit(metadata, sha)
        insert_git_commit(filesystem, sha)
        install.run(self)

setup(cmdclass={'install':SetupWrapper})

