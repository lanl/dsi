import fileinput
import re
from setuptools import setup
from setuptools.command.install import install
import subprocess


# Get version from main folder
exec(open("dsi/_version.py").read())

class SetupWrapper(install):
    def run(self):
        install.run(self)


setup(
    packages=['dsi','dsi.plugins','dsi.backends','dsi.tests'],
    cmdclass={'install': SetupWrapper},
    version=__version__
)
