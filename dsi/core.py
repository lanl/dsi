from importlib import import_module
from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from itertools import product


class Terminal():
    """
    An instantiated Terminal is the DSI human/machine interface.

    Terminals are a home for Plugins and an interface for Backends. Backends may be
    front-ends or back-ends. Plugins may be Writers or readers. See documentation
    for more information.
    """
    BACKEND_PREFIX = ['dsi.backends']
    BACKEND_IMPLEMENTATIONS = ['gufi', 'sqlite', 'parquet']
    PLUGIN_PREFIX = ['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS = ['env', 'file_reader']
    VALID_PLUGINS = ['Hostname', 'SystemKernel', 'GitInfo', 'Bueno', 'Csv']
    VALID_BACKENDS = ['Gufi', 'Sqlite', 'Parquet']
    VALID_MODULES = VALID_PLUGINS + VALID_BACKENDS
    VALID_MODULE_FUNCTIONS = {'plugin': [
        'writer', 'reader'], 'backend': ['front-end', 'back-end']}
    VALID_ARTIFACT_INTERACTION_TYPES = ['get', 'set', 'put', 'inspect']

    def __init__(self):
        # Helper function to get parent module names.
        def static_munge(prefix, implementations):
            return (['.'.join(i) for i in product(prefix, implementations)])

        self.module_collection = {}
        backend_modules = static_munge(
            self.BACKEND_PREFIX, self.BACKEND_IMPLEMENTATIONS)
        self.module_collection['backend'] = {}
        for module in backend_modules:
            self.module_collection['backend'][module] = import_module(module)

        plugin_modules = static_munge(
            self.PLUGIN_PREFIX, self.PLUGIN_IMPLEMENTATIONS)
        self.module_collection['plugin'] = {}
        for module in plugin_modules:
            self.module_collection['plugin'][module] = import_module(module)

        self.active_modules = {}
        valid_module_functions_flattened = self.VALID_MODULE_FUNCTIONS['plugin'] + \
            self.VALID_MODULE_FUNCTIONS['backend']
        for valid_function in valid_module_functions_flattened:
            self.active_modules[valid_function] = []
        self.active_metadata = OrderedDict()
        self.transload_lock = False

    def list_available_modules(self, mod_type):
        """
        List available DSI modules of an arbitrary module type.

        This method is useful for Core Terminal setup. Plugin and Backend type DSI modules
        are supported, but this getter can be extended to support any new DSI module
        types which are added. Note: self.VALID_MODULES refers to _DSI_ Modules
        however, DSI Modules are classes, hence the naming idiosynchrocies below.
        """
        # "DSI Modules" are Python Classes
        class_collector = []
        # Below, "module" refers to Python modules,
        for python_module, classlist in self.module_collection[mod_type].items():
            # In the next line, both "class" and VALID_MODULES refer to DSI modules.
            class_collector.extend(
                [x for x in dir(classlist) if x in self.VALID_MODULES])
        return (class_collector)

    def load_module(self, mod_type, mod_name, mod_function, **kwargs):
        """
        Load a DSI module from the available Plugin and Backend module collection.

        DSI modules may be loaded which are not explicitly listed by the list_available_modules.
        This flexibility ensures that advanced users can access higher level abstractions.
        We expect most users will work with module implementations rather than templates, but
        but all high level class abstractions are accessible with this method.
        """
        if self.transload_lock and mod_type == 'plugin':
            print('Plugin module loading is prohibited after transload. No action taken.')
            return
        if mod_function not in self.VALID_MODULE_FUNCTIONS[mod_type]:
            print(
                'Hint: Did you declare your Module Function in the Terminal Global vars?')
            raise NotImplementedError
        if mod_name in [obj.__class__.__name__ for obj in self.active_modules[mod_function]]:
            print('{} {} already loaded as {}. Nothing to do.'.format(
                mod_name, mod_type, mod_function))
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
            print('{} {} {} loaded successfully.'.format(
                mod_name, mod_type, mod_function))
        else:
            print('Hint: Did you declare your Plugin/Backend in the Terminal Global vars?')
            raise NotImplementedError

    def unload_module(self, mod_type, mod_name, mod_function):
        """
        Unloads a DSI module from the active_modules collection
        """
        if self.transload_lock and mod_type == 'plugin':
            print(
                'Plugin  module unloading is prohibited after transload. No action taken.')
            return
        for i, mod in enumerate(self.active_modules[mod_function]):
            if mod.__class__.__name__ == mod_name:
                self.active_modules[mod_function].pop(i)
                print("{} {} {} unloaded successfully.".format(
                    mod_name, mod_type, mod_function))
                return
        print("{} {} {} could not be found in active_modules. No action taken.".format(
            mod_name, mod_type, mod_function))

    def add_external_python_module(self, mod_type, mod_name, mod_path):
        """
        Adds an external, meaning not from the DSI repo, Python module to the module_collection.

        Afterwards, load_module can be used to load a DSI module from the added Python module.
        Note: mod_type is needed because each Python module only implements plugins or backends.

        For example,

        term = Terminal()
        term.add_external_python_module('plugin', 'my_python_file',

                                        '/the/path/to/my_python_file.py')

        term.load_module('plugin', 'MyPlugin', 'reader')

        term.list_loaded_modules() # includes MyPlugin
        """
        mod = SourceFileLoader(mod_name, mod_path).load_module()
        self.module_collection[mod_type][mod_name] = mod

    def list_loaded_modules(self):
        """
        List DSI modules which have already been loaded.

        These Plugins and Backends are active or ready to execute a post-processing task.
        """
        return (self.active_modules)

    def transload(self, **kwargs):
        """
        Transloading signals to the DSI Core Terminal that Plugin set up is complete.

        A DSI Core Terminal must be transloaded before queries, metadata collection, or metadata
        storage is possible. Transloading is the process of merging Plugin metadata from many
        data sources to a single DSI Core Middleware data structure.
        """
        selected_function_modules = dict(
            (k, self.active_modules[k]) for k in ('writer', 'reader'))
        # Note this transload supports plugin.env Environment types now.
        for module_type, objs in selected_function_modules.items():
            for obj in objs:
                obj.add_rows(**kwargs)
                for col_name, col_metadata in obj.output_collector.items():
                    self.active_metadata[col_name] = col_metadata

        # Plugins may add one or more rows (vector vs matrix data).
        # You may have two or more plugins with different numbers of rows.
        # Consequently, transload operations may have unstructured shape for
        # some plugin configurations. We must force structure to create a valid
        # middleware data structure.
        # To resolve, we pad the shorter columns to match the max length column.
        max_len = max([len(col) for col in self.active_metadata.values()])
        for colname, coldata in self.active_metadata.items():
            if len(coldata) != max_len:
                self.active_metadata[colname].extend(  # add None's until reaching max_len
                    [None] * (max_len - len(coldata)))

        assert all([len(col) == max_len for col in self.active_metadata.values(
        )]), "All columns must have the same number of rows"

        self.transload_lock = True

    def artifact_handler(self, interaction_type, **kwargs):
        """
        Store or retrieve using all loaded DSI Backends with storage functionality.

        A DSI Core Terminal may load zero or more Backends with storage functionality.
        Calling artifact_handler will execute all back-end functionality currently loaded, given
        the provided ``interaction_type``.
        """
        if interaction_type not in self.VALID_ARTIFACT_INTERACTION_TYPES:
            print(
                'Hint: Did you declare your artifact interaction type in the Terminal Global vars?')
            raise NotImplementedError
        operation_success = False
        # Perform artifact movement first, because inspect implementation may rely on
        # self.active_metadata or some stored artifact.
        selected_function_modules = dict(
            (k, self.active_modules[k]) for k in (['back-end']))
        for module_type, objs in selected_function_modules.items():
            for obj in objs:
                if interaction_type == 'put' or interaction_type == 'set':
                    obj.put_artifacts(
                        collection=self.active_metadata, **kwargs)
                    operation_success = True
                elif interaction_type == 'get':
                    self.active_metadata = obj.get_artifacts(**kwargs)
                    operation_success = True
        if interaction_type == 'inspect':
            for module_type, objs in selected_function_modules.items():
                for obj in objs:
                    obj.put_artifacts(
                        collection=self.active_metadata, **kwargs)
                    self.active_metadata = obj.inspect_artifacts(
                        collection=self.active_metadata, **kwargs)
                    operation_success = True
        if operation_success:
            return
        else:
            print(
                'Hint: Did you implement a case for your artifact interaction in the \
                 artifact_handler loop?')
            raise NotImplementedError
