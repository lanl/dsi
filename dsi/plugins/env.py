"""Environment Plugin drivers.

A home for environment plugin parsers.
"""

from collections import OrderedDict, defaultdict
import os
import socket
import subprocess

from dsi.plugins.structured_metadata_plugin import Plugin

class EnvPlugin(Plugin):

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
        
class HostnamePlugin(EnvPlugin):
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
        self.output = defaultdict(list) 


    def parse(self) -> None:
        pass


    def add_row(self) -> None:
        """
        Parses environment provenance data and stores the
        key-value pairs in self.output_collector
        """
        self.update_output(self.get_kernel_version())
        
        self.update_output(self.get_kernel_ct_config())

        self.update_output(self.get_kernel_bt_config())

        self.update_output(self.get_kernel_rt_config())

        self.update_output(self.get_kernel_mod_config())

        self.update_output(self.get_container_config())

        self.output_collector.update(self.output)

    def get_kernel_version(self) -> dict:
        """
        Kernel version is obtained by the "uname -r" command, returns it in a dict
        """
        return {"kernel version": self.get_cmd_output(["uname -r"])}


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
        /proc/cmdline. The output of this command is one string of 
        boot-time parameters. This string is returned in a dict.
        """
        command = ["cat /proc/cmdline"]
        res = self.get_cmd_output(command)
        return {"cat /proc/cmdline": res}


    def get_kernel_rt_config(self) -> dict:
        """
        Kernel run-time configuration is collected with the "sysctl -a" command.
        The output of this command is lines consisting of two possibilities:
            option = value (note the spaces)
            sysctl: permission denied ...
        The option = value pairs are added to the output dict.
        """
        command = ["sysctl -a 2>&1"]
        res = self.get_cmd_output(command)
        lines = res.split("\n")
        rt_config = {}
        for line in lines:
            if "=" in line: # if the line is not permission denied
                option, value = line.split(" = ", maxsplit=1) # note the spaces
                rt_config[option] = value
            # If the line is permission denied, ignore it :(
        return rt_config

    
    def get_kernel_mod_config(self) -> dict:
        """
        Kernel module configuration is collected with the "lsmod" and "modinfo" commands.
        Each module and modinfo are stored as a key-value pair in the returned dict.
        """
        command = ["lsmod | tail -n +2 | awk '{print $1}'"]
        modules = self.get_cmd_output(command)
        mod_list = modules.split("\n")
        mod_configs = {}
        for module in mod_list:
            modinfo_cmd = f"modinfo {module}"
            modinfo = self.get_cmd_output([modinfo_cmd])
            mod_configs[modinfo_cmd] = modinfo
        return mod_configs


    def get_container_config(self) -> dict:
        #TODO: Implement
        return {}


    def update_output(self, pairs: dict) -> None:
        """
        Appends a given dict's values to the output under the same key.
        """
        for key, val in pairs.items():
            self.output[key].append(val)


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


