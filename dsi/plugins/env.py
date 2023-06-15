from collections import OrderedDict
import os
import socket

from dsi.plugins.structured_data import StructuredDataPlugin

class EnvPlugin(StructuredDataPlugin):
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
