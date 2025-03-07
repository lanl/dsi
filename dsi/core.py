from importlib import import_module
from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from itertools import product
import os
import shutil
from pathlib import Path
import logging
from datetime import datetime
import warnings
import sys

from dsi.backends.filesystem import Filesystem
from dsi.backends.sqlite import Sqlite, DataType, Artifact


class Terminal():
    """
    An instantiated Terminal is the DSI human/machine interface.

    Terminals are a home for Plugins and an interface for Backends. Backends may be
    back-reads or back-writes. Plugins may be writers or readers. See documentation
    for more information.
    """
    BACKEND_PREFIX = ['dsi.backends']
    BACKEND_IMPLEMENTATIONS = ['gufi', 'sqlite', 'parquet']
    PLUGIN_PREFIX = ['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS = ['env', 'file_reader', 'file_writer']
    VALID_ENV = ['Hostname', 'SystemKernel', 'GitInfo']
    VALID_READERS = ['Bueno', 'Csv', 'YAML1', 'TOML1', 'Schema', 'MetadataReader1', 'Wildfire']
    VALID_WRITERS = ['ER_Diagram', 'Table_Plot', 'Csv_Writer']
    VALID_PLUGINS = VALID_ENV + VALID_READERS + VALID_WRITERS
    VALID_BACKENDS = ['Gufi', 'Sqlite', 'Parquet']
    VALID_MODULES = VALID_PLUGINS + VALID_BACKENDS
    VALID_MODULE_FUNCTIONS = {'plugin': ['reader', 'writer'], 
                              'backend': ['back-read', 'back-write']}
    VALID_ARTIFACT_INTERACTION_TYPES = ['put', 'get', 'inspect', 'read', 'ingest', 'query', 'notebook', 'process']

    def __init__(self, debug = 0, backup_db = False, runTable = False):
        """
        Initialization helper function to pass through optional parameters for DSI core.

        Optional flags can be set and defined:

        `debug`: {0: off, 1: user debug log, 2: user + developer debug log} 
            
            - When set to 1 or 2, debug info will write to a local debug.log text file with various benchmarks.
        `backup_db`: Undefined False as default. If set to True, this creates a backup database before committing new changes.

        `runTable`: Undefined False as default. 
        When new metadata is ingested, a 'runTable' is created, appended, and timestamped when database in incremented. Recommended for in-situ use-cases.
        """
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
        valid_module_functions_flattened = self.VALID_MODULE_FUNCTIONS['plugin'] + self.VALID_MODULE_FUNCTIONS['backend']
        for valid_function in valid_module_functions_flattened:
            self.active_modules[valid_function] = []
        self.active_metadata = OrderedDict()
        self.loaded_backends = []

        self.runTable = runTable
        self.backup_db = backup_db

        self.logger = logging.getLogger(self.__class__.__name__)
        self.debug_level = debug
        if debug == 1 or debug == 2:
            logging.basicConfig(
                filename='debug.log',         # Name of the log file
                filemode='w',               # Overwrite mode ('w' for overwrite, 'a' for append)
                format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
                level=logging.INFO          # Minimum log level to capture
            )

    def list_available_modules(self, mod_type):
        """
        List available DSI modules of an arbitrary module type.

        This method is useful for Core Terminal setup. Plugin and Backend type DSI modules
        are supported, but this getter can be extended to support any new DSI module
        types which are added. 
        
        Note: self.VALID_MODULES refers to _DSI_ Modules
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

        If a loaded module has mod_type='plugin' & mod_function='reader', it is automatically activated and then unloaded as well.
        Therefore, a user does not have to activate it separately with transload() (only used by plugin writers) or call unload_module()
        """
        if self.debug_level != 0:
            self.logger.info(f"-------------------------------------")
            self.logger.info(f"Loading {mod_name} {mod_function} {mod_type}")
        start = datetime.now()
        if mod_type not in ["plugin", "backend"]:
            if self.debug_level != 0:
                self.logger.error("Your module type was not a 'plugin' or 'backend'")
            raise NotImplementedError('Hint: All module types must be either "plugin" or "backend"')
        if mod_function not in self.VALID_MODULE_FUNCTIONS[mod_type]:
            if self.debug_level != 0:
                self.logger.error(f"Your module function was not found in VALID_MODULE_FUNCTIONS['{mod_type}']")
            raise NotImplementedError('Hint: Did you declare your Module Function in the Terminal Global vars?')
        if mod_type == "plugin" and (mod_function in self.VALID_MODULE_FUNCTIONS["backend"] or mod_name in self.VALID_BACKENDS):
            if self.debug_level != 0:
                self.logger.error("You are trying to load a mismatched plugin. Please check the VALID_MODULE_FUNCTIONS and VALID_PLUGINS again")
            raise ValueError("You are trying to load a mismatched plugin. Please check the VALID_MODULE_FUNCTIONS and VALID_PLUGINS again")
        if mod_type == "backend" and (mod_function in self.VALID_MODULE_FUNCTIONS["plugin"] or mod_name in self.VALID_PLUGINS):
            if self.debug_level != 0:
                self.logger.error("You are trying to load a mismatched backend. Please check the VALID_MODULE_FUNCTIONS and VALID_BACKENDS again")
            raise ValueError("You are trying to load a mismatched backend. Please check the VALID_MODULE_FUNCTIONS and VALID_BACKENDS again")
        
        load_success = False
        for python_module in list(self.module_collection[mod_type].keys()):
            try:
                this_module = import_module(python_module)
                class_ = getattr(this_module, mod_name)
                load_success = True
                
                if mod_function == "reader":
                    try:
                        obj = class_(**kwargs)
                    except:
                        if self.debug_level != 0:
                            self.logger.error(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                        raise TypeError(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                    
                    run_start = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info("   Activating this reader in load_module")
                    
                    try:
                        sys.settrace(self.trace_function) # starts a short trace to get line number where plugin reader returned
                        ingest_error = obj.add_rows()
                        if ingest_error is not None:
                            if self.debug_level != 0:
                                self.logger.error(f"   {ingest_error[1]}")
                            raise ingest_error[0](f"Caught error in {original_file} @ line {return_line_number}: " + ingest_error[1])
                        sys.settrace(None) # ends trace to prevent large overhead
                    except:
                        if self.debug_level != 0:
                            self.logger.error(f'   Data structure error in add_rows() of {mod_name} plugin. Check to ensure data was stored correctly')
                        raise RuntimeError(f'Data structure error in add_rows() of {mod_name} plugin. Check to ensure data was stored correctly')
                    
                    for table_name, table_metadata in obj.output_collector.items():
                        if "hostname" in table_name.lower():
                            for colName, colData in table_metadata.items():
                                if isinstance(colData[0], list):
                                    str_list = []
                                    for val in colData:
                                        str_list.append(f'{val}')
                                    table_metadata[colName] = str_list
                        if table_name not in self.active_metadata.keys():
                            self.active_metadata[table_name] = table_metadata
                        else:
                            for colName, colData in table_metadata.items():
                                if colName in self.active_metadata[table_name].keys() and table_name != "dsi_units":
                                    self.active_metadata[table_name][colName] += colData
                                elif colName in self.active_metadata[table_name].keys() and table_name == "dsi_units":
                                    for key, col_unit in colData.items():
                                        if key not in self.active_metadata[table_name][colName]:
                                            self.active_metadata[table_name][colName][key] = col_unit
                                        elif key in self.active_metadata[table_name][colName] and self.active_metadata[table_name][colName][key] != col_unit:
                                            if self.debug_level != 0:
                                                self.logger.error(f"   Cannot have a different set of units for column {key} in {colName}")
                                            raise ValueError(f"Cannot have a different set of units for column {key} in {colName}")
                                elif colName not in self.active_metadata[table_name].keys():
                                    self.active_metadata[table_name][colName] = colData
                    run_end = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info(f"   Activated this reader with runtime: {run_end-run_start}")

                else:
                    try:
                        if mod_type == "backend" and hasattr(class_, 'runTable'):
                            class_.runTable = self.runTable
                        class_object = class_(**kwargs)
                        self.active_modules[mod_function].append(class_object)
                        if mod_type == "backend":
                            self.loaded_backends.append(class_object)
                    except:
                        if self.debug_level != 0:
                            self.logger.error(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                        raise TypeError(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                
                print(f'{mod_name} {mod_type} {mod_function} loaded successfully.')
                end = datetime.now()
                if self.debug_level != 0:
                    self.logger.info(f"{mod_name} {mod_function} {mod_type} loaded successfully.")
                    self.logger.info(f"Runtime: {end-start}")
                break
            except AttributeError:
                continue

        if not load_success:
            if self.debug_level != 0:
                self.logger.error("Plugin/Backend not found in VALID_PLUGINS/VALID_BACKENDS")
            raise NotImplementedError('Hint: Did you declare your Plugin/Backend in VALID_PLUGINS/VALID_BACKENDS?')

    def unload_module(self, mod_type, mod_name, mod_function):
        """
        Unloads a specific DSI module from the active_modules collection.

        Mostly to be used for unloading backends, as plugin readers and writers are auto unloaded elsewhere.
        """
        if self.debug_level != 0:
            self.logger.info(f"-------------------------------------")
            self.logger.info(f"Unloading {mod_name} {mod_function} {mod_type}")
        start = datetime.now()

        if mod_type not in ["plugin", "backend"]:
            if self.debug_level != 0:
                self.logger.error("Your module type was not a 'plugin' or 'backend'")
            raise NotImplementedError('Hint: All module types must be either "plugin" or "backend"')
        if mod_function not in self.VALID_MODULE_FUNCTIONS[mod_type]:
            if self.debug_level != 0:
                self.logger.error(f"Your module function was not found in VALID_MODULE_FUNCTIONS['{mod_type}']")
            raise NotImplementedError(f'Hint: Did you declare your Module Function in VALID_MODULE_FUNCTIONS["{mod_type}"]?')
        
        indices = []
        for i, mod in enumerate(self.active_modules[mod_function]):
            if mod.__class__.__name__ == mod_name:
                indices.append(i)
        
        if len(indices) > 0:
            last_loaded = indices[-1]
            if mod_type == 'backend':
                self.active_modules[mod_function][last_loaded].close()
                loaded_index = self.loaded_backends.index(self.active_modules[mod_function][last_loaded])
                self.loaded_backends[loaded_index].close()
                self.loaded_backends.pop(loaded_index)
            self.active_modules[mod_function].pop(last_loaded)
            print(f"{mod_name} {mod_type} {mod_function} unloaded successfully.")
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"{mod_name} {mod_function} {mod_type} unloaded successfully.")
                self.logger.info(f"Runtime: {end-start}")
        else:            
            warnings.warn(f"{mod_name} {mod_type} {mod_function} could not be found in active_modules. No action taken.")
            if self.debug_level != 0:
                self.logger.warning(f"{mod_name} {mod_function} {mod_type} could not be found in active_modules. No action taken.")

    def add_external_python_module(self, mod_type, mod_name, mod_path):
        """
        Adds an external, meaning not from the DSI repo, Python module to the module_collection. 
        Afterwards, load_module can be used to load a DSI module from the added Python module.

        Note: mod_type is needed because each Python module only implements plugins or backends.

        Check Example 7 in Core:Examples on GitHub Docs to see how to use this function.
        """
        mod = SourceFileLoader(mod_name, mod_path).load_module()
        self.module_collection[mod_type][mod_name] = mod
        self.VALID_MODULES.append(mod_name)

    def list_loaded_modules(self):
        """
        List DSI modules which have already been loaded.

        These Plugins and Backends are active or ready to execute a post-processing task.
        """
        return (self.active_modules)

    def transload(self, **kwargs):
        """
        Transloading signals to the DSI Core Terminal that Plugin set up is complete.

        Activates all loaded plugin writers by generating all their various output files such as an ER Diagram or an image of a table plot

        All loaded plugin writers will be unloaded after activation, so there is no need to separately call unload_module() for them
        """
        used_writers = []
        for obj in self.active_modules['writer']:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Transloading {obj.__class__.__name__} {'writer'}")
            start = datetime.now()
            try:
                sys.settrace(self.trace_function) # starts a short trace to get line number where writer plugin returned
                writer_error = obj.get_rows(self.active_metadata, **kwargs)
                if writer_error is not None:
                    if writer_error[0] == "Warning":
                        warnings.warn(writer_error[1])
                    else:
                        if self.debug_level != 0:
                            self.logger.error(writer_error[1])
                        raise writer_error[0](f"Caught error in {original_file} @ line {return_line_number}: " + writer_error[1])
                sys.settrace(None) # ends trace to prevent large overhead
            except:
                if self.debug_level != 0: 
                    self.logger.error(f'   Data structure error in get_rows() of {obj.__class__.__name__} plugin. Check to ensure data was handled correctly')
                raise RuntimeError(f'Data structure error in get_rows() of {obj.__class__.__name__} plugin. Check to ensure data was handled correctly')
            used_writers.append(obj)
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
        unused_writers = list(set(self.active_modules["writer"]) - set(used_writers))
        if len(unused_writers) > 0:
            self.active_modules["writer"] = unused_writers
            if self.debug_level != 0:
                self.logger.warning(f"Not all plugin writers were successfully transloaded. These were not transloaded: {unused_writers}")
            warnings.warn(f"Not all plugin writers were successfully transloaded. These were not transloaded: {unused_writers}")
        else:
            self.active_modules["writer"] = []

    def close(self):
        """
        Immediately closes all active modules: backends, plugin writers, plugin readers

        Clears out the current DSI abstraction

        NOTE - This step cannot be undone.
        """
        print("Closing the abstraction layer, and all active plugins/backends")
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info("Closing and clearing out all objects in this Terminal object")
            
            self.logger.info("Cleared out the abstraction layer")
        self.active_metadata = OrderedDict()

        if self.debug_level != 0:
            self.logger.info("Closed active backends")
        active_backends = self.active_modules['back-write'] + self.active_modules['back-read']
        for backend in active_backends:
            backend.close()
        for loaded in self.loaded_backends:
            loaded.close()

        if self.debug_level != 0:
            self.logger.info("Cleared all loaded plugins and backends")
        for func in self.active_modules:
            self.active_modules[func] = []
            self.loaded_backends = []

    def artifact_handler(self, interaction_type, query = None, **kwargs):
        """
        Interact with loaded DSI backends by ingesting or retrieving data from them.
        
        `interaction_type`:

            - 'ingest' or 'put': ingests active DSI abstraction into ALL loaded BACK-WRITE backends (BACK-READ backends ignored

                - if backup_db flag = True in a local Core, a backup is created prior to ingesting data into each loaded backend
            - 'query' or 'get': retrieves data from first loaded backend based on a specified 'query'
            - 'notebook' or 'inspect': generates an interactive Python notebook with all data from first loaded backend
            - 'process' or 'read': overwrites current DSI abstraction with all data from first loaded BACK-READ backend

        `query`: default None. Specify if *interaction_type* = 'query' and query_artifact function in backend file requires an input

        A DSI Core Terminal may load zero or more Backends with storage functionality.
        """
        if interaction_type not in self.VALID_ARTIFACT_INTERACTION_TYPES:
            if self.debug_level != 0:
                self.logger.error('Could not find artifact interaction type in VALID_ARTIFACT_INTERACTION_TYPES')
            raise NotImplementedError('Hint: Did you declare your artifact interaction type in the Terminal Global vars?')
        
        operation_success = False
        backread_active = False
        if interaction_type in ['ingest', 'put']:
            for obj in self.active_modules['back-write']:
                if self.debug_level != 0:
                    self.logger.info("-------------------------------------")
                    self.logger.info(f"{obj.__class__.__name__} backend - {interaction_type.upper()} the data")
                start = datetime.now()
                parent_class = obj.__class__.__bases__[0].__name__
                if self.backup_db == True and parent_class == "Filesystem" and os.path.getsize(obj.filename) > 100:
                    if self.debug_level != 0:
                        self.logger.info(f"   Creating backup file before ingesting data into {obj.__class__.__name__} backend")
                    backup_start = datetime.now()
                    formatted_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
                    backup_file = obj.filename[:obj.filename.rfind('.')] + "_backup_" + formatted_datetime + obj.filename[obj.filename.rfind('.'):]
                    shutil.copyfile(obj.filename, backup_file)
                    backup_end = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info(f"   Backup file runtime: {backup_end-backup_start}")
                
                sys.settrace(self.trace_function) # starts a short trace to get line number where ingest_artifacts() returned 
                if interaction_type == "ingest":
                    errorMessage = obj.ingest_artifacts(collection = self.active_metadata, **kwargs)
                elif interaction_type == "put":
                    errorMessage = obj.put_artifacts(collection = self.active_metadata, **kwargs)
                if errorMessage is not None:
                    if self.debug_level != 0:
                        self.logger.error(f"Error ingesting data in {original_file} @ line {return_line_number} due to {errorMessage[1]}")
                    raise errorMessage[0](f"Error ingesting data in {original_file} @ line {return_line_number} due to {errorMessage[1]}")
                sys.settrace(None) # ends trace to prevent large overhead
                operation_success = True
                end = datetime.now()
                self.logger.info(f"Runtime: {end-start}")
        if interaction_type in ['ingest', 'put'] and len(self.active_modules['back-read']) > 0:
            backread_active = True

        query_data = None
        first_backend = self.loaded_backends[0]
        if interaction_type not in ['ingest', 'put', "processs", "read"] and self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"{first_backend.__class__.__name__} backend - {interaction_type.upper()} the data")
        start = datetime.now()
        if interaction_type in ['query', 'get']:
            if "query" in first_backend.query_artifacts.__code__.co_varnames:
                self.logger.info(f"Query to get data: {query}")
                kwargs['query'] = query

            sys.settrace(self.trace_function) # starts a short trace to get line number where query_artifacts() returned
            if interaction_type == "get":
                query_data = first_backend.get_artifacts(**kwargs)
            elif interaction_type == "query":
                query_data = first_backend.query_artifacts(**kwargs)
            if isinstance(query_data, tuple):
                if self.debug_level != 0:
                    self.logger.error(query_data[1])
                raise query_data[0](f"Caught error in {original_file} @ line {return_line_number}: " + query_data[1])
            sys.settrace(None) # ends trace to prevent large overhead
            operation_success = True

        elif interaction_type in ['notebook', 'inspect']:
            parent_class = first_backend.__class__.__bases__[0].__name__
            if parent_class == "Filesystem" and os.path.getsize(first_backend.filename) > 100:
                try:
                    if interaction_type == "inspect":
                        first_backend.inspect_artifacts(**kwargs)
                    elif interaction_type == "notebook":
                        first_backend.notebook(**kwargs)
                except:
                    raise ValueError("Error in generating notebook. Please ensure data in the actual backend is stable")
            elif parent_class == "Connection": # NEED ANOTHER CHECKER TO SEE IF BACKEND IS NOT EMPTY WHEN BACKEND IS NOT A FILESYSTEM
                pass
            else: #backend is empty - cannot inspect
                if self.debug_level != 0:
                    self.logger.error("Error in notebook/inspect artifact handler: Need to ingest data into a backend before generating Jupyter notebook")
                raise ValueError("Error in notebook/inspect artifact handler: Need to ingest data into a backend before generating Jupyter notebook")
            operation_success = True

        elif interaction_type in ["process", "read"] and len(self.active_modules['back-read']) > 0:
            first_backread = self.active_modules['back-read'][0]
            if self.debug_level != 0:
                self.logger.info(f"{first_backread.__class__.__name__} backend - {interaction_type.upper()} the data")
            if interaction_type == "process":
                self.active_metadata = first_backread.process_artifacts()
            elif interaction_type == "read":
                self.active_metadata = first_backread.read_to_artifact()
            operation_success = True
        elif interaction_type in ["process", "read"] and len(self.active_modules['back-read']) == 0:
            backread_active = True

        if operation_success:
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"Runtime: {end-start}")
            if interaction_type in ['query', 'get'] and query_data is not None:
                return query_data
        else:
            not_run_msg = None
            if backread_active:
                not_run_msg = 'Remember that back-WRITE backends cannot process/read data and back-READ backends cannot ingest/put'
            else:
                not_run_msg = 'Is your artifact interaction implemented in your specified backend?'
            if self.debug_level != 0:
                self.logger.error(not_run_msg)
            raise NotImplementedError(not_run_msg)
    
    # Internal function used to get line numbers from return statements - should not be called by users
    def trace_function(self, frame, event, arg):
        global return_line_number
        global original_file
        if event == "return":
            return_line_number = frame.f_lineno  # Get line number
            original_file = frame.f_code.co_filename # Get file name
        return self.trace_function
    
    def find(self, query_object):
        """
        Find all function that searches for all instances of 'query_object' in first loaded backend. Searches among all tables/column/cells

        `return`: List of backend-specific objects that each contain details of a match for 'query_object'

            - check file of the first backend loaded to understand the structure of the objects in this list
        """
        backend = self.loaded_backends[0]
        start = datetime.now()
        return_object = backend.find(query_object)
        return self.find_helper(query_object, return_object, start, "")
    
    def find_table(self, query_object):
        """
        Find table function that searches for all tables whose names matches the 'query_object' in first loaded backend.

        `return`: List of backend-specific objects that each contain all data from a table matching 'query_object'.

            - check file of the first backend loaded to understand the structure of the objects in this list
        """
        backend = self.loaded_backends[0]
        start = datetime.now()
        return_object = backend.find_table(query_object)
        return self.find_helper(query_object, return_object, start, "table ")
    
    def find_column(self, query_object, range = False):
        """
        Find column function that searches for all columns whose names matches the 'query_object' in first loaded backend.

        `range`: default False. 

            - If True, then data-range of all numerical columns which match 'query_object' is included in return
            - If False, then data for each column that matches 'query_object' is included in return
        `return`: List of backend-specific objects that each contain data/numerical range about a column matching 'query_object'.

            - check file of the first backend loaded to understand the structure of the objects in this list
        """
        backend = self.loaded_backends[0]
        start = datetime.now()
        return_object = backend.find_column(query_object, range)
        return self.find_helper(query_object, return_object, start, "column ")

    def find_cell(self, query_object, row = False):
        """
        Find cell function that searches for all cells which match the 'query_object' in first loaded backend.

        `row`: default False.

            - If True, then full row of data where a cell matches 'query_object' is included in return
            - If False, then the value of the cell that matches 'query_object' is included in return
        `return`: List of backend-specific objects that each contain value of a cell/full row where a cell matches 'query_object'

            - check file of the first backend loaded to understand the structure of the objects in this list
        """
        backend = self.loaded_backends[0]
        start = datetime.now()
        return_object = backend.find_cell(query_object, row)
        return self.find_helper(query_object, return_object, start, "cell ")

    def find_helper(self, query_object, return_object, start, find_type):
        """
        **Users should not call this externally, only to be used by internal core functions.**
        
        Helper function to print/log information for all core find functions: find(), find_table(), find_column(), find_cell()
        """
        if isinstance(query_object, str): 
            print(f"Finding all {find_type}matches of '{query_object}' in first backend loaded")
        else:
            print(f"Finding all {find_type}matches of {query_object} in first backend loaded")
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Finding all instances of '{query_object}' in first backend loaded in")
        if isinstance(return_object, tuple):
            if self.debug_level != 0:
                self.logger.warning(return_object[1])
            warnings.warn(return_object[1])
            return_object = return_object[0]
        else:
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"Runtime: {end-start}")
        return return_object
    
    def get_current_abstraction(self, table_name = None):
        """
        Returns the current DSI abstraction as a nested Ordered Dict, where keys are table names and values are the table's data as an Ordered Dict

        The inner table Ordered Dict has column names as keys and list of column data as the values.

        `table_name`: default None. If specified, the return will only be that table's Ordered Dict, not a nested one.

        `return`: nested Ordered Dict if table_name is None. single Ordered Dict if table_name is not None
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Returning current abstraction")
        start = datetime.now()
        if table_name is not None and table_name not in self.active_metadata.keys():
            if self.debug_level != 0:
                self.logger.error(f"{table_name} not in current abstraction")
            raise ValueError(f"{table_name} not in current abstraction")
        if table_name is not None:
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"Runtime: {end-start}")
            return self.active_metadata[table_name]
        else:
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"Runtime: {end-start}")
            return self.active_metadata
        
    def update_abstraction(self, table_name, table_data):
        """
        Updates the DSI abstraction, by overwriting the specified table_name with the input table_data

        `table_name`: name of table that must be in the current abstraction

        `table_data`: table data that must be stored as an Ordered Dict where column names are keys and column data is a list stored as values.
        
        `return`: None
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Updating abstraction table {table_name} with new data")
        if table_name not in self.active_metadata.keys():
            if self.debug_level != 0:
                self.logger.error(f"{table_name} not in current abstraction")
            raise ValueError(f"{table_name} not in current abstraction")
        if not isinstance(table_data, OrderedDict):
            if self.debug_level != 0:
                self.logger.error("table_data needs to be in the form of an Ordered Dictionary")
            raise ValueError("table_data needs to be in the form of an Ordered Dictionary")
        self.active_metadata[table_name] = table_data
            

class Sync():
    """
    A class defined to assist in data management activities for DSI

    Sync is where data movement functions such as copy (to remote location) and 
    sync (local filesystem with remote) exist.
    """
    remote_location = []
    local_location = []

    def __init__(self, project_name="test"):
        # Helper function to get parent module names.
        #self.remote_location = {}
        #self.local_location = {}
        self.project_name = project_name

    def populate(self, local_loc, remote_loc, isVerbose=False):
        """
        Helper function to gather filesystem information, local and remote locations
        to create a filesystem entry in a new or existing database
        """
        True

    def copy(self, local_loc, remote_loc, isVerbose=False):
        """
        Helper function to stage location and get filesystem information, and copy
        data over using a preferred API
        """
        if isVerbose:
            print("loc: "+local_loc+ " rem: "+remote_loc)
        # Data Crawl and gather metadata of local location
        file_list = self.dircrawl(local_loc)

        # populate st_list to hold all filesystem attributes
        st_list = []
        rfile_list = []

        # Do a quick validation of group permissions
        for file in file_list:
            file = os.path.relpath(file,local_loc) #rel path
            #utils.isgroupreadable(file) # quick test
            filepath = os.path.join(local_loc, file)
            st = os.stat(filepath)
            # append future location to st
            rfilepath = os.path.join(remote_loc,self.project_name, file)
            rfile_list.append(rfilepath)
            st_list.append(st)

        # Test remote location validity, try to check access
        # Future: iterate through remote/server list here, for now:::
        remote_list = [ os.path.join(remote_loc,self.project_name) ]
        for remote in remote_list:
            try: # Try for file permissions
                if os.path.exists(remote): # Check if exists
                    print(f"The directory '{remote}' already exists locally.")
                else:
                    os.makedirs(remote) # Create it
                    print(f"The directory '{remote}' has been created locally.")
            except Exception as err:
                print(f"Unexpected {err=}, {type(err)=}")
                raise

        # Try to open or create a local database to store fs info before copy
        # Open and validate local DSI data store
        try:
            #f = os.path.join((local_loc, str(self.project_name+".db") ))
            f = local_loc+"/"+self.project_name+".db"
            print("db: ", f)
            store = Sqlite( f )
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

        # Create new filesystem table with origin and remote locations
        data_type = DataType()
        data_type.name = "filesystem"
        data_type.properties["file_origin"] = Sqlite.STRING
        data_type.properties["st_mode"] = Sqlite.DOUBLE
        data_type.properties["st_ino"] = Sqlite.DOUBLE
        data_type.properties["st_dev"] = Sqlite.DOUBLE
        data_type.properties["st_nlink"] = Sqlite.DOUBLE
        data_type.properties["st_uid"] = Sqlite.DOUBLE
        data_type.properties["st_gid"] = Sqlite.DOUBLE
        data_type.properties["st_size"] = Sqlite.DOUBLE
        data_type.properties["st_atime"] = Sqlite.DOUBLE
        data_type.properties["st_mtime"] = Sqlite.DOUBLE
        data_type.properties["st_ctime"] = Sqlite.DOUBLE
        data_type.properties["file_remote"] = Sqlite.STRING
        #print(data_type.properties)
        store.put_artifact_type(data_type, isVerbose)

        artifact = Artifact()
        artifact.name = "filesystem"
        for file,st,file_remote in zip(file_list,st_list,rfile_list):
            artifact.properties["file_origin"] = str(file)
            artifact.properties["st_mode"] = st.st_mode
            artifact.properties["st_ino"] = st.st_ino
            artifact.properties["st_dev"] = st.st_dev
            artifact.properties["st_nlink"] = st.st_nlink
            artifact.properties["st_uid"] = st.st_uid
            artifact.properties["st_gid"] = st.st_gid
            artifact.properties["st_size"] = st.st_size
            artifact.properties["st_atime"] = st.st_atime
            artifact.properties["st_mtime"] = st.st_mtime
            artifact.properties["st_ctime"] = st.st_ctime
            artifact.properties["file_remote"] = str(file_remote)
            #print(artifact.properties)
            store.put_artifacts_lgcy(artifact, isVerbose)

        store.close()

        # Data movement
        # Future: have movement service handle type (cp,scp,ftp,rsync,etc.)
        for file,file_remote in zip(file_list,rfile_list):
            abspath = os.path.dirname(os.path.abspath(file_remote))
            if not os.path.exists(abspath):
                if isVerbose:
                    print( " mkdir " + abspath)
                path = Path(abspath)
                path.mkdir(parents=True)

            if isVerbose:
                print( " cp " + file + " " + file_remote)
            shutil.copyfile(file , file_remote)
            

        # Database movement
        if isVerbose:
            print( " cp " + os.path.join(local_loc, str(self.project_name+".db") ) + " " + os.path.join(remote_loc, self.project_name, self.project_name+".db" ) )
        shutil.copyfile(os.path.join(local_loc, str(self.project_name+".db") ), os.path.join(remote_loc, self.project_name, self.project_name+".db" ) )

        print( " Data Copy Complete! ")


    def dircrawl(self,filepath):
        """
        Crawls the root 'filepath' directory and returns files

        `filepath`: source filepath to be crawled

        `return`: returns crawled file-list
        """
        file_list = []
        for root, dirs, files in os.walk(filepath):
            #if os.path.basename(filepath) != 'tmp': # Lets skip some files
            #    continue

            for f in files: # Appent root-level files
                file_list.append(os.path.join(root, f))
            for d in dirs: # Recursively dive into directories
                sub_list = self.dircrawl(os.path.join(root, d))
                for sf in sub_list:
                    file_list.append(sf)
    
        return file_list
    
    def get(project_name = "Project"):
        '''
        Helper function that searches remote location based on project name, and retrieves
        DSI database
        '''
        True
        
