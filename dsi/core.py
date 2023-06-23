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
    DRIVER_PREFIX = ['dsi.drivers']
    DRIVER_IMPLEMENTATIONS = ['gufi','sqlite']
    PLUGIN_PREFIX = ['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS = ['env']
    VALID_PLUGINS = ['Hostname','SystemKernel','Bueno']
    VALID_DRIVERS = ['Gufi','Sqlite']
    VALID_MODULES = VALID_PLUGINS + VALID_DRIVERS

    def __init__(self):
        # Helper function to get parent module names.
        static_munge = \
        lambda prefix,implementations: ['.'.join(i) for i in product(prefix,implementations)]

        self.module_collection = {}
        driver_modules = static_munge(self.DRIVER_PREFIX, self.DRIVER_IMPLEMENTATIONS)
        self.module_collection['driver'] = {}
        for module in driver_modules:
            self.module_collection['driver'][module] = import_module(module)

        plugin_modules = static_munge(self.PLUGIN_PREFIX, self.PLUGIN_IMPLEMENTATIONS)
        self.module_collection['plugin'] = {}
        for module in plugin_modules:
            self.module_collection['plugin'][module] = import_module(module)

        self.active_modules = []

    def list_available_modules(self,mod_type):
        """
        List available modules of an arbitrary module type.

        This method is useful for interactive Core Terminal setup. Plugin and Driver
        type modules are supported, but this getter can be extended to support any
        new modules which are added. Note: self.VALID_MODULES refers to _DSI_ Modules
        however, DSI Modules are classes, hence the naming convention idosynchrocies
        below.
        """
        # Helper function to determine if module is in a collection
        class_collector = []
        for module,classlist in self.module_collection[mod_type].items():
           class_collector.extend([x for x in dir(classlist) if x in self.VALID_MODULES])
        return(class_collector)

    def load_module(mod_type, mod_name):
        if mod_type=='plugin' and mod_name in self.plugin_collection:
            print('Plugin {} already loaded. Nothing to do.'.format(mod_name))
#        else:
#            for plugin in self.plugin_collection:
#                try:
#                    plugin.
##                 
# Core capability requirements:
# 	Init plugin, parse data
# 	Store backend
# 	Query backend
# 	SQLAlchemy converts query to backend compatible format
# 	Only supports python primitive data structures, everything else goes into a driver.
