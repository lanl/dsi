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
    VALID_MODULE_FUNCTIONS = {'plugin':['producer','consumer'],'driver':['front-end','back-end']}

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

        self.active_modules = {}
        valid_module_functions_flattened = self.VALID_MODULE_FUNCTIONS['plugin'] + self.VALID_MODULE_FUNCTIONS['driver']
        for valid_function in valid_module_functions_flattened:
            self.active_modules[valid_function] = []

    def list_available_modules(self, mod_type):
        """
        List available DSI modules of an arbitrary module type.

        This method is useful for Core Terminal setup. Plugin and Driver type DSI modules 
        are supported, but this getter can be extended to support any new DSI module 
        types which are added. Note: self.VALID_MODULES refers to _DSI_ Modules
        however, DSI Modules are classes, hence the naming idiosynchrocies below.
        """
        # "DSI Modules" are Python Classes
        class_collector = []
        # Below, "module" refers to Python modules,
        for python_module,classlist in self.module_collection[mod_type].items():
           # In the next line, both "class" and VALID_MODULES refer to DSI modules.
           class_collector.extend([x for x in dir(classlist) if x in self.VALID_MODULES])
        return(class_collector)

    def load_module(self, mod_type, mod_name, mod_function, **kwargs):
        """
        Load a DSI module from the available Plugin and Drive module collection.

        DSI modules may be loaded which are not explicitly listed by the list_available_modules.
        This flexibility ensures that advanced users can access higher level abstractions.
        We expect most users will work with module implementations rather than templates, but
        but all high level class abstractions are accessible with this method.
        """
        if mod_function not in self.VALID_MODULE_FUNCTIONS[mod_type]:
            raise NotImplementedError
        if mod_name in [obj.__class__.__name__ for obj in self.active_modules[mod_function]]:
           print('{} {} already loaded as {}. Nothing to do.'.format(mod_name,mod_type,mod_function))
           return   
        # DSI Modules are Python classes.
        class_name = mod_name
        load_success = False
        for python_module in list(self.module_collection[mod_type].keys()):
            try:
                this_module = import_module(python_module)
                class_ = getattr(this_module, class_name)
                self.active_modules[mod_function].append(class_(**kwargs))
                load_success = True
            except AttributeError:
                continue
        if load_success:
            print('{} {} {} loaded successfully.'.format(mod_name,mod_type,mod_function))
        else:
            raise NotImplementedError
                
    def list_loaded_modules(self):
        """
        List DSI modules which have already been loaded.

        These Plugins and Drivers are active or ready to execute a post-processing task.
        """
        return(self.active_modules)

    def transload(self):
        """
        Transloading signals to the DSI Core Terminal that Plugin/Driver set up is complete.

        A DSI Core Terminal must be transloaded before queries, metadata collection, or metadata 
        storage is possible. Transloading is the process of merging Plugin metadata from many 
        data sources to a single DSI Core Middleware data structure.
        """
        pass

    def query(self):
        """Query is the user-facing query interface to a DSI Core Terminal."""
        pass

    def store(self):
        """
        Store using all loaded DSI Drivers with back-end functionality.

        A DSI Core Terminal may load zero or more Drivers with back-end storage functionality.
        Calling store will execute all back-end functionality currently loaded.
        """
        pass

