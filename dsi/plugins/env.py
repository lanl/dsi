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
        """Load valid environment plugin file.

        Environment plugin files assume a POSIX-compliant
        filesystem and always collect UID/GID information.
        """
        super().__init__()


    def parse(self) -> None:
        """
        Parses environment provenance data and stores the
        key-value pairs in self.output_collector
        """
        k_ver = self.get_kernel_version()
        self.output_collector["kernel version"] = k_ver
        
        k_ct_conf = self.get_kernel_ct_config()
        self.output_collector.update(k_ct_conf)

        k_bt_conf = self.get_kernel_bt_config()
        self.output_collector.update(k_bt_conf)

        #k_rt_conf = self.get_kernel_rt_config()
        #TODO: modules
        #TODO: containers
        for key in self.output_collector:
            print(key, ": ", self.output_collector[key])
        print(len(self.output_collector))


    def add_row(self) -> None:
        #TODO: What would add_row do in this context?
        pass


    def get_kernel_version(self) -> str:
        """
        Kernel version is obtained by the "uname -r" command as a str
        """
        return self.get_cmd_output(["uname -r"])


    def get_kernel_ct_config(self) -> dict:
        """
        Kernel compile-time configuration is collected by looking at 
        /boot/config-(kernel version) and removing comments and empty lines. 
        The output of said command is newline-delimited option=value pairs.
        """
        command = ["cat /boot/config-$(uname -r) | sed -e '/#/d' -e '/^$/d'"]
        res = self.get_cmd_output(command)
        lines = res.split("\n")
        ct_config = {}
        for line in lines: # each line is an option=value pair
            option, value = line.split("=", maxsplit=1)
            ct_config[option] = value
        return ct_config


    def get_kernel_bt_config(self) -> dict:
        """
        Kernel boot-time configuration is collected by looking at
        /proc/cmdline. The output of this is space-delimited option=value pairs
        and potentially standalone options, namely "ro".
        Standalone options are given the value True in the output dict.
        """
        command = ["cat /proc/cmdline"]
        res = self.get_cmd_output(command)
        lines = res.split(" ")
        bt_config = {}
        for line in lines: # each line is either "option=value" or "option"
            if "=" in line:
                option, value = line.split("=", maxsplit=1)
                bt_config[option] = value
            else:
                bt_config[line] = True # standalone option
        return bt_config


    def get_kernel_rt_config(self) -> dict:
        """
        Kernel run-time configuration is collected with the "sysctl -a" command.
        The output of this command is lines consisting of two possibilities:
            option = value (note the spaces)
            sysctl: permission denied ...
        The option = value pairs are added to the output dict.
        """
        command = ["sysctl -a"]
        res = self.get_cmd_output(command)
        lines = res.split("\n")
        rt_config = {}
        for line in lines:
            if "=" in line: # if the line is not permission denied
                option, value = line.split(" = ", maxsplit=1) # note the spaces
                rt_config[option] = value
            # What to do when sysctl: permission denied?
        return rt_config


    @staticmethod
    def get_cmd_output(cmd: list) -> str:
        """
        Runs a given command and returns the stdout if successful.
        If stderr is not empty, an exception is raised with the stderr text.
        """
        proc = subprocess.run(cmd, capture_output=True, shell=True)
        if proc.stderr != b"":
            raise Exception(proc.stderr)
        return proc.stdout.strip().decode("utf-8")


