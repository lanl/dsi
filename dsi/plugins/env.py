"""Environment Plugin drivers.

A home for environment plugin parsers.
"""

import os

from dsi.plugins.driver import PluginDriver

class EnvPluginDriver(PluginDriver):

    def __init__(self, path):
        """Load valid environment plugin file.

        Environment plugin files assume a POSIX-compliant
        filesystem and always collect UID/GID information.
        """
        # Get POSIX info
        self.posix_info = {}
        self.posix_info['uid'] = os.getuid()
        self.posix_info['effective_gid'] = os.getgid()
        self.posix_info['moniker'] = os.getlogin()
        self.posix_info['gid_list'] = os.getgrouplist(os.getlogin(),
                                                      os.getgid())
        
class HostnamePluginDriver(EnvPluginDriver):
    """An example Plugin implementation.

    This plugin collects the hostname of the machine,
    the UID and effective GID of the plugin collector, and
    the Unix time of the collected information.
    """

    def __init__(self, path):
        """Load valid environment plugin file.

        Environment plugin files assume a POSIX-compliant
        filesystem and always collect UID/GID information.
        """
