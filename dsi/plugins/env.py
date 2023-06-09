"""Environment Plugin drivers.

A home for environment plugin parsers.
"""

from collections import OrderedDict
import os
import socket

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
