from importlib import import_module
from itertools import product
import os

class Terminal():
    """
    An instantiated Terminal is the DSI human/machine interface.

    Terminals are a home for Plugins and an interface for Drivers. Drivers may be
    front-ends or back-ends. Plugins may be producers or consumers. See documentation
    for more information.
    """
    DRIVER_PREFIX=['dsi.drivers']
    DRIVER_IMPLEMENTATIONS=['gufi','sqlite']
    PLUGIN_PREFIX=['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS=['env']
    VALID_PLUGINS=['Hostname','SystemKernel','Bueno']
    VALID_DRIVERS=['Gufi','Sqlite']

    def __init__(self):
        # Helper function to get parent module names.
        static_munge = \
        lambda prefix,implementations: ['.'.join(i) for i in product(prefix,implementations)]

        self.drivers = []
        driver_modules = static_munge(self.DRIVER_PREFIX, self.DRIVER_IMPLEMENTATIONS)
        for module in driver_modules:
            self.drivers.append(import_module(module))

        self.plugins = []
        plugin_modules = static_munge(self.PLUGIN_PREFIX, self.PLUGIN_IMPLEMENTATIONS)
        for module in plugin_modules:
            self.plugins.append(import_module(module))

    def list_available_modules(self,mod_type):
        """
        List available modules of an arbitrary module type.

        This method is useful for interactive Core Terminal setup. Plugin and Driver
        type modules are supported, but this getter can be extended to support any
        new modules which are added.
        """
        # Helper function to determine if module is in a collection
        mod_is_present = lambda modname, mod_collection: \
        sum([modname in dir(mod) for mod in mod_collection])
        valid_mods=[]
        if mod_type=='plugin':
            for valid_plugin in self.VALID_PLUGINS:
                if mod_is_present(valid_plugin, self.plugins):
                    valid_mods.append(valid_plugin)
        if mod_type=='driver':
            for valid_driver in self.VALID_DRIVERS:
                if mod_is_present(valid_driver, self.drivers):
                    valid_mods.append(valid_driver)
        print('Loadable {}s: {}'.format(mod_type,', '.join(valid_mods)))
          
# Core capability requirements:
# 	Init plugin, parse data
# 	Store backend
# 	Query backend
# 	SQLAlchemy converts query to backend compatible format
# 	Only supports python primitive data structures, everything else goes into a driver.
