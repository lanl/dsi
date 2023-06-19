"""Environment Plugin drivers.

A home for environment plugin parsers.
"""

from collections import OrderedDict 
import os
import socket
import subprocess

from dsi.plugins.metadata import StructuredMetadataPlugin

class EnvPlugin(StructuredMetadataPlugin):
    """Environment Plugins inspect the calling process' context.
    
    EnvPlugins assume a POSIX-compliant filesystem and always collect UID/GID
    information.
    """

    def __init__(self, path=None):
        super().__init__()
        # Get POSIX info
        self.posix_info = OrderedDict()
        self.posix_info['uid'] = os.getuid()
        self.posix_info['effective_gid'] = os.getgid()
        egid = self.posix_info['effective_gid']
        self.posix_info['moniker'] = os.getlogin()
        moniker = self.posix_info['moniker']
        self.posix_info['gid_list'] = os.getgrouplist(moniker, egid)

class HostnamePlugin(EnvPlugin):
    """An example EnvPlugin implementation.

    This plugin collects the hostname of the machine, and couples this with the POSIX
    information gathered by the EnvPlugin base class.
    """

    def __init__(self) -> None:
        super().__init__()

    def pack_header(self) -> None:
        column_names = list(self.posix_info.keys()) + ["hostname"]
        self.set_schema(column_names)

    def add_row(self) -> None:
        if not self.schema_is_set():
            self.pack_header()

        row = list(self.posix_info.values()) + [socket.gethostname()]
        self.add_to_output(row)



class EnvProvPlugin(EnvPlugin):
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
        """ Initialize EnvProvPlugin with inital provenance info """
        super().__init__()
        self.prov_info = self.get_prov_info()


    def pack_header(self) -> None:
        """ Set schema with keys of prov_info """
        column_names = list(self.prov_info.keys())
        self.set_schema(column_names)


    def add_row(self) -> None:
        """
        Parses environment provenance data and adds the row
        """
        if not self.schema_is_set():
            self.pack_header()

        pairs = self.get_prov_info()
        self.add_to_output(list(pairs.values()))
        

    def get_prov_info(self) -> dict:
        """ collect and return the different categories of provenance info  """
        prov_info = {}
        prov_info.update(self.get_kernel_version())
        prov_info.update(self.get_kernel_ct_config())
        prov_info.update(self.get_kernel_bt_config())
        prov_info.update(self.get_kernel_rt_config())
        prov_info.update(self.get_kernel_mod_config())
        prov_info.update(self.get_container_config())
        return prov_info


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
            modinfo_cmd = f"/sbin/modinfo {module}"
            modinfo = self.get_cmd_output([modinfo_cmd])
            mod_configs[modinfo_cmd] = modinfo
        return mod_configs


    def get_container_config(self) -> dict:
        # Not yet implemented
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


