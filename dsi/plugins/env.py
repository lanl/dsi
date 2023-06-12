"""Environment Plugin drivers.

A home for environment plugin parsers.
"""

from collections import OrderedDict
import os
import socket
import subprocess

from dsi.plugins.driver import PluginDriver

class EnvPluginDriver(PluginDriver):

    def __init__(self, path=None) -> None:
        """Load valid environment plugin file.

        Environment plugin files assume a POSIX-compliant
        filesystem and always collect UID/GID information.
        """
        # Get POSIX info
        self.posix_info = OrderedDict()
        self.posix_info['uid'] = os.getuid()
        self.posix_info['effective_gid'] = os.getgid()
        egid = self.posix_info['effective_gid']
        self.posix_info['moniker'] = os.getlogin()
        moniker = self.posix_info['moniker']
        self.posix_info['gid_list'] = os.getgrouplist(moniker, egid)
        # Plugin output collector
        self.output_collector = OrderedDict()
        
class HostnamePlugin(EnvPluginDriver):
    """An example Plugin implementation.

    This plugin collects the hostname of the machine,
    the UID and effective GID of the plugin collector, and
    the Unix time of the collected information.
    """

    def __init__(self) -> None:
        """Load valid environment plugin file.

        Environment plugin files assume a POSIX-compliant
        filesystem and always collect UID/GID information.
        """
        super().__init__()

    def add_row(self) -> None:
        row = list(self.posix_info.values()) + [socket.gethostname()]
        for key,row_elem in zip(self.output_collector, row):
            self.output_collector[key].append(row_elem) 

    def parse(self) -> None:
        for key in self.posix_info:
            self.output_collector[key] = []
        self.output_collector['hostname'] = []
        self.add_row()



class EnvProvPlugin(EnvPluginDriver):
    """
    Plugin for reading environment provenance data:
    1. System Kernel Version
    2. Kernel compile-time config
    3. Kernel boot config
    4. Kernel runtime config
    5. Kernel modules and module config
    6. Container information, if containerized
    """

    def __init__(self) -> None:
        super().__init__()


    def parse(self) -> None:
        k_ver = self.get_kernel_version()
        #k_ct_conf = self.get_kernel_ct_config()
        #k_bt_conf = self.get_kernel_bt_config()
        #k_rt_conf = self.get_kernel_rt_config()
        #TODO: modules
        #TODO: containers
        self.output_collector["kernel version"] = k_ver

    def add_row(self) -> None:
        #TODO: figure out what goes here
        pass

    def get_kernel_version(self) -> str:
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


