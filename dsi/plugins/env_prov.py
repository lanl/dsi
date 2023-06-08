#!/usr/bin/python

import subprocess

from plugin import Plugin


class EnvProvPlugin(Plugin):
    """
    Plugin for reading environment provenance data:
    1. System Kernel Version
    2. Kernel compile-time config
    3. Kernel boot config
    4. Kernel runtime config
    5. Kernel modules and module config
    6. Container information, if containerized
    """

    def __init__(self):
        super().__init__()


    def read_data(self, **kwargs) -> dict:
        """
        Override method to rea environment provenance data

        #TODO: There will be arguments for container info
        """
        k_ver = self.get_kernel_version()
        k_ct_conf = self.get_kernel_ct_config()
        k_bt_conf = self.get_kernel_bt_config()
        k_rt_conf = self.get_kernel_rt_config()
        #TODO: modules
        #TODO: containers


    def get_kernel_version(self):
        return self.get_cmd_output(["uname", "-r"])


    def get_kernel_ct_config(self):
        pass


    def get_kernel_bt_config(self):
        pass


    def get_kernel_rt_config(self):
        pass


    @staticmethod
    def get_cmd_output(cmd: list) -> str:
        proc = subprocess.run(cmd, capture_output=True)
        if proc.stderr != b"":
            raise Exception(proc.stderr)
        return proc.stdout.strip().decode("utf-8")



