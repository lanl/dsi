from collections import OrderedDict
import os
import socket
import subprocess
from getpass import getuser
from json import dumps

from dsi.plugins.metadata import StructuredMetadata
from dsi.plugins.plugin_models import (
    EnvironmentModel, GitInfoModel, HostnameModel, SystemKernelModel, create_dynamic_model
)


class Environment(StructuredMetadata):
    """
    Environment Plugins inspect the calling process' context.

    Environments assume a POSIX-compliant filesystem and always collect UID/GID
    information.
    """

    def __init__(self):
        super().__init__()
        # Get POSIX info
        self.posix_info = OrderedDict()
        self.posix_info['uid'] = os.getuid()
        self.posix_info['effective_gid'] = os.getgid()
        egid = self.posix_info['effective_gid']
        self.posix_info['moniker'] = getuser()
        moniker = self.posix_info['moniker']
        self.posix_info['gid_list'] = os.getgrouplist(moniker, egid)


class Hostname(Environment):
    """
    An example Environment implementation.

    This plugin collects the hostname of the machine, and couples this with the POSIX
    information gathered by the Environment base class.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__()

    def pack_header(self) -> None:
        """Set schema with keys of prov_info."""
        column_names = list(self.posix_info.keys()) + ["hostname"]
        self.set_schema(column_names, validation_model=HostnameModel)

    def add_rows(self) -> None:
        """Parses environment provenance data and adds the row."""
        if not self.schema_is_set():
            self.pack_header()

        row = list(self.posix_info.values()) + [socket.gethostname()]
        self.add_to_output(row)


class GitInfo(Environment):
    """
    A Plugin to capture Git information.

    Adds the current git remote and git commit to metadata.
    """

    def __init__(self, git_repo_path='./') -> None:
        """ Initializes the git repo in the given directory and access to git commands """
        super().__init__()
        import git.exc
        try:
            from git import Repo
            self.repo = Repo(git_repo_path, search_parent_directories=True)
        except git.exc.InvalidGitRepositoryError:
            raise Exception(f"Git could not find .git/ in {git_repo_path}, " +
                            "GitInfo Plugin must be given a repo base path " +
                            "(default is working dir)")
        self.git_info = {
            "git_remote": lambda: self.repo.git.remote("-v"),
            "git_commit": lambda: self.repo.git.rev_parse("--short", "HEAD")
        }

    def pack_header(self) -> None:
        """ Set schema with POSIX and Git columns """
        column_names = list(self.posix_info.keys()) + \
            list(self.git_info.keys())
        self.set_schema(column_names, validation_model=GitInfoModel)

    def add_rows(self) -> None:
        """ Adds a row to the output with POSIX info, git remote, and git commit """
        if not self.schema_is_set():
            self.pack_header()

        row = list(self.posix_info.values()) + \
            [self.git_info["git_remote"](), self.git_info["git_commit"]()]
        self.add_to_output(row)


class SystemKernel(Environment):
    """
    Plugin for reading environment provenance data.

    An environment provenance plugin which does the following:

    1. System Kernel Version

    2. Kernel compile-time config

    3. Kernel boot config

    4. Kernel runtime config

    5. Kernel modules and module config
    
    6. Container information, if containerized
    """

    def __init__(self) -> None:
        """Initialize SystemKernel with inital provenance info."""
        super().__init__()
        self.prov_info = self.get_prov_info()
        self.column_names = ["kernel_info"]

    def pack_header(self) -> None:
        """Set schema with keys of prov_info."""
        column_names = list(self.posix_info.keys()) + self.column_names
        self.set_schema(column_names, validation_model=SystemKernelModel)

    def add_rows(self) -> None:
        """Parses environment provenance data and adds the row."""
        if not self.schema_is_set():
            self.pack_header()

        blob = self.get_prov_info()
        self.add_to_output(list(self.posix_info.values()) + [blob])

    def get_prov_info(self) -> str:
        """Collect and return the different categories of provenance info."""
        prov_info = {}
        prov_info.update(self.get_kernel_version())
        prov_info.update(self.get_kernel_ct_config())
        prov_info.update(self.get_kernel_bt_config())
        prov_info.update(self.get_kernel_rt_config())
        prov_info.update(self.get_kernel_mod_config())
        blob = dumps(prov_info)
        return blob

    def get_kernel_version(self) -> dict:
        """Kernel version is obtained by the "uname -r" command, returns it in a dict. """
        return {"kernel version": self.get_cmd_output(["uname -r"])}

    def get_kernel_ct_config(self) -> dict:
        """
        Kernel compile-time configuration is collected by looking at /boot/config-(kernel version)
        and removing comments and empty lines.

        The output of said command is newline-delimited option=value pairs.
        """
        command = ["cat /boot/config-$(uname -r) | sed -e '/#/d' -e '/^$/d'"]
        res = self.get_cmd_output(command)
        lines = res.split("\n")
        ct_config = {}
        for line in lines:  # each line is an option=value pair
            option, value = line.split("=", maxsplit=1)
            ct_config[option] = value
        return ct_config

    def get_kernel_bt_config(self) -> dict:
        """
        Kernel boot-time configuration is collected by looking at /proc/cmdline.

        The output of this command is one string of boot-time parameters. This string is
        returned in a dict.
        """
        command = ["cat /proc/cmdline"]
        res = self.get_cmd_output(command)
        return {"cat /proc/cmdline": res}

    def get_kernel_rt_config(self) -> dict:
        """
        Kernel run-time configuration is collected with the "sysctl -a" command.

        The output of this command is lines consisting of two possibilities:
        option = value (note the spaces), and sysctl: permission denied ...
        The option = value pairs are added to the output dict.
        """
        command = ["sysctl -a 2>&1"]
        res = self.get_cmd_output(command)
        lines = res.split("\n")
        rt_config = {}
        for line in lines:
            if "=" in line:  # if the line is not permission denied
                option, value = line.split(
                    " = ", maxsplit=1)  # note the spaces
                rt_config[option] = value
            # If line is permission denied, ignore it.
        return rt_config

    def get_kernel_mod_config(self) -> dict:
        """
        Kernel module configuration is collected with the "lsmod" and "modinfo" commands.

        Each module and modinfo are stored as a key-value pair in the returned dict.
        """
        lsmod_command = ["lsmod | tail -n +2 | awk '{print $1}'"]
        modules = self.get_cmd_output(lsmod_command).split("\n")

        sep = "END MODINFO"
        modinfo_command = ["/sbin/modinfo $(lsmod | tail -n +2 | awk '{print $1}' | \
                           sed 's/nvidia_/nvidia-current-/g' | \
                           sed 's/^nvidia$/nvidia-current/g') | "
                           f"sed -e 's/filename:/{sep}filename:/g'"]
        modinfos = self.get_cmd_output(
            modinfo_command, ignore_stderr=True).split("\n" + sep)

        mod_configs = {}
        for mod, info in zip(modules, modinfos):
            mod_configs["/sbin/modinfo " + mod] = info
        return mod_configs

    @staticmethod
    def get_cmd_output(cmd: list, ignore_stderr=False) -> str:
        """
        Runs a given command and returns the stdout if successful.

        If stderr is not empty, an exception is raised with the stderr text.
        """
        proc = subprocess.run(cmd, capture_output=True, shell=True)
        if proc.stderr != b"" and not ignore_stderr:
            raise Exception(proc.stderr)
        return proc.stdout.strip().decode("utf-8")
