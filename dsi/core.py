from importlib import import_module
from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from itertools import product
import os
import shutil
from pathlib import Path
import logging
from datetime import datetime
import sys
import pandas as pd
import csv
import re

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
    BACKEND_IMPLEMENTATIONS = ['gufi', 'sqlite', 'parquet', 'duckdb']
    PLUGIN_PREFIX = ['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS = ['env', 'file_reader', 'file_writer']
    VALID_ENV = ['Hostname', 'SystemKernel', 'GitInfo']
    VALID_READERS = ['Bueno', 'Csv', 'YAML1', 'TOML1', 'Schema', 'JSON', 'MetadataReader1', 'Ensemble', 'Cloverleaf']
    VALID_DATACARDS = ['Oceans11Datacard', 'DublinCoreDatacard', 'SchemaOrgDatacard', 'GoogleDatacard']
    VALID_WRITERS = ['ER_Diagram', 'Table_Plot', 'Csv_Writer']
    VALID_PLUGINS = VALID_ENV + VALID_READERS + VALID_WRITERS + VALID_DATACARDS
    VALID_BACKENDS = ['Gufi', 'Sqlite', 'Parquet', 'DuckDB', 'SqlAlchemy']
    VALID_MODULES = VALID_PLUGINS + VALID_BACKENDS
    VALID_MODULE_FUNCTIONS = {'plugin': ['reader', 'writer'], 
                              'backend': ['back-read', 'back-write']}
    VALID_ARTIFACT_INTERACTION_TYPES = ['put', 'get', 'inspect', 'read', 'ingest', 'query', 'notebook', 'process']

    def __init__(self, debug = 0, backup_db = False, runTable = False):
        """
        Initialization function to configure optional DSI core parameters.

        Optional flags
        --------------
        `debug` : int, default=0 
            {0: off, 1: user debug log, 2: user + developer debug log} 
            
            When set to 1 or 2, debug info will write to a local debug.log text file with various benchmarks.

        `backup_db` : bool, default=False
            - If True, creates a backup of the current backend database before committing any new changes.

        `runTable` : bool, default=False
            - If True, a 'runTable' is created, and timestamped each time new data/metadata is ingested.
              Recommended for in-situ use-cases.
        """
        def static_munge(prefix, implementations):
            return (['.'.join(i) for i in product(prefix, implementations)])

        self.module_collection = {}
        backend_modules = static_munge(self.BACKEND_PREFIX, self.BACKEND_IMPLEMENTATIONS)
        self.module_collection['backend'] = {}
        for module in backend_modules:
            try:
                imported = import_module(module)
                self.module_collection['backend'][module] = imported
            except ImportError as e:
                continue

        plugin_modules = static_munge(self.PLUGIN_PREFIX, self.PLUGIN_IMPLEMENTATIONS)
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

        self.user_wrapper = False
        self.new_tables = None
        self.dsi_tables = ["runtable", "filesystem", "oceans11_datacard", "dublin_core_datacard", "schema_org_datacard", "google_datacard"]

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
        are supported, but this getter can be extended to support any new DSI module types which are added. 
        
        Note: self.VALID_MODULES refers to _DSI_ Modules however, DSI Modules are classes, hence the naming idiosynchrocies below.
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
        if mod_type == "backend" and not any(mod_name.lower() in item for item in self.module_collection[mod_type].keys()):
            if self.debug_level != 0:
                self.logger.error("You are trying to load a backend that is not installed in a base dsi setup. Please run requirements.extras.txt")
            raise ValueError("You are trying to load a backend that is not installed in a base dsi setup. Please run requirements.extras.txt")
        if mod_type == "plugin" and mod_name.lower() == "wildfire":
            mod_name = "Ensemble"
        if mod_type == "plugin" and mod_name.lower() == "csv":
            mod_name = "Csv"
        if mod_type == "plugin" and mod_name.lower() == "csv_writer":
            mod_name = "Csv_Writer"
        
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
                            self.logger.error(f'The kwargs for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                        raise ValueError(f'The kwargs for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                    
                    run_start = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info("   Activating this reader in load_module")
                    
                    tester = 0
                    if sys.gettrace() is None:
                        tester = 1
                        sys.settrace(self.trace_function) # starts a short trace to get line number where plugin reader returned

                    ingest_error = None
                    try:
                        ingest_error = obj.add_rows()
                    except:
                        if self.debug_level != 0:
                            self.logger.error(f'   Data structure error in add_rows() of {mod_name} plugin. Check to ensure data was stored correctly')
                        raise RuntimeError(f'Data structure error in add_rows() of {mod_name} plugin. Check to ensure data was stored correctly')
                    
                    if ingest_error is not None:
                            if self.debug_level != 0:
                                self.logger.error(f"   {ingest_error[1]}")
                            if self.user_wrapper:
                                raise ingest_error[0](ingest_error[1])
                            else:
                                raise ingest_error[0](f"Caught error in {original_file} @ line {return_line_number}: " + ingest_error[1])
                    if tester == 1:
                        sys.settrace(None) # ends trace to prevent large overhead
                    
                    self.new_tables = obj.output_collector.keys()
                    for table_name, table_metadata in obj.output_collector.items():
                        if table_name.lower() == "runtable":
                            if self.debug_level != 0:
                                self.logger.error(f"   Cannot read in '{table_name}' — runTable is a reserved DSI table name.")
                            raise RuntimeError(f"Cannot read in '{table_name}' — runTable is a reserved DSI table name.")
                        if "hostname" in table_name.lower():
                            for colName, colData in table_metadata.items():
                                if isinstance(colData[0], list):
                                    str_list = []
                                    for val in colData:
                                        str_list.append(f'{val}')
                                    table_metadata[colName] = str_list
                        if table_name == "dsi_units":
                            incorrect_cols = set(["table_name", "column_name", "unit"]).issubset(table_metadata.keys())
                            if len(table_metadata.keys()) != 3 or incorrect_cols == False:
                                if self.debug_level != 0:
                                    self.logger.error(f"   'dsi_units' table columns MUST be: 'table_name', 'column_name', 'unit'")
                                raise TypeError(f"'dsi_units' table columns MUST be: 'table_name', 'column_name', 'unit'")
                        if table_name not in self.active_metadata.keys():
                            self.active_metadata[table_name] = table_metadata
                        else:
                            for colName, colData in table_metadata.items():
                                if colName in self.active_metadata[table_name].keys():
                                    self.active_metadata[table_name][colName] += colData
                                else:
                                    self.active_metadata[table_name][colName] = colData
                            if table_name == "dsi_units":
                                t_list = self.active_metadata[table_name]['table_name']
                                c_list = self.active_metadata[table_name]['column_name']
                                u_list = self.active_metadata[table_name]['unit']
                                visited = {}
                                for t_name, c_name, unit in zip(t_list, c_list, u_list):
                                    key = (t_name, c_name)
                                    if key in visited and visited[key] != unit:
                                        if self.debug_level != 0:
                                            self.logger.error(f"   Cannot have a different set of units for column {c_name} in {t_name}")
                                        raise TypeError(f"Cannot have a different set of units for column {c_name} in {t_name}")
                                    visited[key] = unit
                    run_end = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info(f"   Activated this reader with runtime: {run_end-run_start}")

                else:
                    try:
                        if mod_type == "backend" and hasattr(class_, 'runTable'):
                            parent_classes = class_.__bases__
                            if parent_classes and parent_classes[0].__name__ == "Filesystem" and 'filename' in kwargs:
                                backend_filename = kwargs['filename']
                                has_data = False
                                has_runTable = False
                                # if to-be-loaded backend has data and runTable in its tables, turn global runTable off
                                if os.path.isfile(backend_filename):
                                    if class_.__name__ == "Sqlite" and os.path.getsize(backend_filename) > 100:
                                        has_data = True
                                    elif class_.__name__ == "DuckDB" and os.path.getsize(backend_filename) > 13000:
                                        has_data = True
                                if has_data:
                                    with open(backend_filename, 'rb') as fb:
                                        content = fb.read()
                                    if b'runTable' in content:
                                        has_runTable = True
                                if has_runTable:
                                    self.runTable = True
                                elif has_data and has_runTable == False and self.runTable == True:
                                    raise ValueError("runTable flag is only valid for in-situ workflows, not for populated backends wihout a runTable.")
                                
                            class_.runTable = self.runTable
                        class_object = class_(**kwargs)
                        self.active_modules[mod_function].append(class_object)
                        if mod_type == "backend":
                            self.loaded_backends.append(class_object)
                    except Exception as e:
                        if "runTable flag is only valid for in-situ workflows" in str(e):
                            if self.debug_level != 0:
                                self.logger.error("runTable flag is only valid for in-situ workflows, not for populated backends wihout a runTable.")
                            raise
                        if self.debug_level != 0:
                            self.logger.error(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                        raise ValueError(f'Specified parameters for {mod_name} {mod_function} {mod_type} were incorrect. Check the class again')
                
                if mod_type == "backend":
                    print(f'{mod_name} {mod_function} {mod_type} loaded successfully.')
                else:
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

        Primarily used when unloading backends, as plugin readers and writers are automatically unloaded elsewhere.
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
            print(f"WARNING: {mod_name} {mod_type} {mod_function} could not be found in active_modules. No action taken.")
            if self.debug_level != 0:
                self.logger.warning(f"{mod_name} {mod_function} {mod_type} could not be found in active_modules. No action taken.")

    def add_external_python_module(self, mod_type, mod_name, mod_path):
        """
        Adds an external, meaning not from the DSI repo, Python module to the module_collection. 
        Afterwards, load_module can be used to load a DSI module from the added Python module.

        Note: mod_type is needed because each Python module only implements plugins or backends.

        View :ref:`external_readers_writers_label` to see how to use this function.
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

        Activates all loaded plugin writers by generating their various output files such as an ER Diagram or an image of a table plot

        All loaded plugin writers will be unloaded after activation, so there is no need to separately call unload_module() for them
        """
        used_writers = []
        for obj in self.active_modules['writer']:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Transloading {obj.__class__.__name__} {'writer'}")
            start = datetime.now()

            tester = 0
            if sys.gettrace() is None:
                tester = 1
                sys.settrace(self.trace_function) # starts a short trace to get line number where writer plugin returned
            
            writer_error = None
            try:
                writer_error = obj.get_rows(self.active_metadata, **kwargs)
            except:
                if self.debug_level != 0: 
                    self.logger.error(f'   Data structure error in get_rows() of {obj.__class__.__name__} plugin. Ensure data was handled correctly')
                raise RuntimeError(f'Data structure error in get_rows() of {obj.__class__.__name__} plugin. Ensure data was handled correctly')
            
            if writer_error is not None:
                if writer_error[0] == "Warning":
                    print("WARNING:", writer_error[1])
                else:
                    if self.debug_level != 0:
                        self.logger.error(writer_error[1])
                    if self.user_wrapper:
                        raise writer_error[0](writer_error[1])
                    else:
                        raise writer_error[0](f"Caught error in {original_file} @ line {return_line_number}: " + writer_error[1])
            if tester == 1:
                sys.settrace(None) # ends trace to prevent large overhead

            used_writers.append(obj)
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
        unused_writers = list(set(self.active_modules["writer"]) - set(used_writers))
        if len(unused_writers) > 0:
            self.active_modules["writer"] = unused_writers
            if self.debug_level != 0:
                self.logger.warning(f"Not all writers were successfully transloaded. These were not transloaded: {unused_writers}")
            print(f"WARNING: Not all writers were successfully transloaded. These were not transloaded: {unused_writers}")
        else:
            self.active_modules["writer"] = []

    def artifact_handler(self, interaction_type, query = None, **kwargs):
        """
        Interact with loaded DSI backends by ingesting or retrieving data from them.
        
        `interaction_type` : str
            Specifies the type of action to perform. Accepted values:

                - 'ingest' or 'put': ingests active DSI abstraction into all loaded BACK-WRITE backends (BACK-READ backends ignored)

                    - if backup_db flag = True in Core instance, a backup is created prior to ingesting data
                - 'query' or 'get': retrieves data from first loaded backend based on a specified 'query'
                - 'notebook' or 'inspect': generates an interactive Python notebook with all data from first loaded backend
                - 'process' or 'read': overwrites current DSI abstraction with all data from first loaded BACK-READ backend

        `query` : str, optional
            Required only when `interaction_type` is 'query' or 'get', and it is an input to a backend's `query_artifact()` method.

        kwargs : 
            Additional keyword arguments passed to underlying backend functions. 
            View relevant functions in the DSI backend file to understand other arguments to pass in.
        
        `return`: only when `interaction_type` = 'query'
            By default stores query result as a Pandas.DataFrame. If specified, returns it as an OrderedDict

        A DSI Core Terminal may load zero or more Backends with storage functionality.
        """
        if interaction_type not in self.VALID_ARTIFACT_INTERACTION_TYPES:
            if self.debug_level != 0:
                self.logger.error('Could not find artifact interaction type in VALID_ARTIFACT_INTERACTION_TYPES')
            raise NotImplementedError('Hint: Did you declare your artifact interaction type in the Terminal Global vars?')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing an action on it')
            raise NotImplementedError('Need to load a valid backend before performing an action on it')
        
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
                        self.logger.info(f"   Creating backup file before ingesting data into the {obj.__class__.__name__} backend")
                    backup_start = datetime.now()
                    backup_file = obj.filename[:obj.filename.rfind('.')] + ".backup" + obj.filename[obj.filename.rfind('.'):]
                    shutil.copyfile(obj.filename, backup_file)
                    backup_end = datetime.now()
                    if self.debug_level != 0:
                        self.logger.info(f"   Backup file runtime: {backup_end-backup_start}")
                
                tester = 0
                if sys.gettrace() is None:
                    tester = 1
                    sys.settrace(self.trace_function) # starts a short trace to get line number where ingest_artifacts() returned 
                if interaction_type == "ingest":
                    errorMessage = obj.ingest_artifacts(collection = self.active_metadata, **kwargs)
                elif interaction_type == "put":
                    errorMessage = obj.put_artifacts(collection = self.active_metadata, **kwargs)
                if errorMessage is not None:
                    if self.debug_level != 0:
                        self.logger.error(f"Error ingesting data in {original_file} @ line {return_line_number} due to {errorMessage[1]}")
                    if self.user_wrapper:
                        raise errorMessage[0](f"Error ingesting data due to {errorMessage[1]}")
                    else:
                        raise errorMessage[0](f"Error ingesting data in {original_file} @ line {return_line_number} due to {errorMessage[1]}")
                if tester == 1:
                    sys.settrace(None) # ends trace to prevent large overhead
                operation_success = True
                end = datetime.now()
                self.logger.info(f"Runtime: {end-start}")
        if interaction_type in ['ingest', 'put'] and len(self.active_modules['back-read']) > 0:
            backread_active = True

        query_data = None
        first_backend = self.loaded_backends[0]
        parent_backend = first_backend.__class__.__bases__[0].__name__
        if interaction_type not in ['ingest', 'put', "process", "read"] and self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"{first_backend.__class__.__name__} backend - {interaction_type.upper()} the data")
        start = datetime.now()
        if interaction_type in ['query', 'get']:
            # Only used when reading data from Parquet backend in CLI API (Parquet uses query not process) - 
            # TODO fix this passthrough by updating Parquet to use process_artifacts()
            # TODO query all backends
            if len(self.loaded_backends) > 1:
                if parent_backend == "Filesystem" and ".temp.db" in first_backend.filename:
                    first_backend = self.loaded_backends[1]
                    parent_backend = first_backend.__class__.__bases__[0].__name__
            if self.valid_backend(first_backend, parent_backend):
                if "query" in first_backend.query_artifacts.__code__.co_varnames:
                    self.logger.info(f"Query to get data: {query}")
                    kwargs['query'] = query
                tester = 0
                if sys.gettrace() is None:
                    tester = 1
                    sys.settrace(self.trace_function) # starts a short trace to get line number where query_artifacts() returned
                if interaction_type == "get":
                    query_data = first_backend.get_artifacts(**kwargs)
                elif interaction_type == "query":
                    query_data = first_backend.query_artifacts(**kwargs)
                if isinstance(query_data, tuple):
                    if self.debug_level != 0:
                        self.logger.error(query_data[1])
                    if self.user_wrapper:
                        raise query_data[0](query_data[1])
                    else:
                        raise query_data[0](f"Caught error in {original_file} @ line {return_line_number}: " + query_data[1])
                if tester == 1:
                    sys.settrace(None) # ends trace to prevent large overhead
                operation_success = True
            else: #backend is empty - cannot query
                if self.debug_level != 0:
                    self.logger.error("Need to ingest data into first loaded backend before querying data from it")
                raise RuntimeError("Need to ingest data into first loaded backend before querying data from it")

        elif interaction_type in ['notebook', 'inspect']:
            if self.valid_backend(first_backend, parent_backend):
                try:
                    if interaction_type == "inspect":
                        first_backend.inspect_artifacts(**kwargs)
                    elif interaction_type == "notebook":
                        first_backend.notebook(**kwargs)
                except:
                    raise RuntimeError("Error in generating notebook. Please ensure data in the actual backend is stable")
            elif parent_backend == "Connection": # NEED ANOTHER CHECKER TO SEE IF BACKEND IS NOT EMPTY WHEN BACKEND IS NOT A FILESYSTEM
                pass
            else: #backend is empty - cannot inspect
                if self.debug_level != 0:
                    self.logger.error("Need to ingest data into first loaded backend before generating a Python notebook")
                raise RuntimeError("Need to ingest data into first loaded backend before generating a Python notebook")
            operation_success = True
        # only processes data from first backend for now - TODO process data from all active backends later
        elif interaction_type in ["process", "read"]:
            if len(self.loaded_backends) > 1:
                if parent_backend == "Filesystem" and ".temp.db" in first_backend.filename:
                    first_backend = self.loaded_backends[1]
                    parent_backend = first_backend.__class__.__bases__[0].__name__
            if self.valid_backend(first_backend, parent_backend):
                if self.debug_level != 0:
                    self.logger.info(f"{first_backend.__class__.__name__} backend - {interaction_type.upper()} the data")
                if interaction_type == "process":
                    self.active_metadata = first_backend.process_artifacts()
                elif interaction_type == "read":
                    self.active_metadata = first_backend.read_to_artifact()
                operation_success = True
            else: #backend is empty - cannot process data
                if self.debug_level != 0:
                    self.logger.error("First loaded backend needs to have data to be able to process data to DSI")
                raise RuntimeError("First loaded backend needs to have data to be able to process data to DSI")

        if operation_success:
            end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"Runtime: {end-start}")
            if interaction_type in ['query', 'get'] and query_data is not None:
                return query_data
        else:
            not_run_msg = None
            if backread_active:
                not_run_msg = 'Remember that back-READ backends cannot ingest/put data'
            else:
                not_run_msg = 'Is your artifact interaction implemented in your specified backend?'
            if self.debug_level != 0:
                self.logger.error(not_run_msg)
            raise NotImplementedError(not_run_msg)    
    
    def get_table(self, table_name, dict_return = False):
        """
        Returns all data from a specified table in the first loaded backend.

        `table_name` : str
            Name of the table to retrieve data from.

        `dict_return`: bool, optional, default=False.
            If True, returns the data as an OrderedDict.
            If False (default), returns the data as a pandas DataFrame.
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Getting data from the table: {table_name} in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend to be able to get data from a specified table')
            raise NotImplementedError('Need to load a valid backend to be able to get data from a specified table')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to get a table")
            raise RuntimeError("First loaded backend needs to have data to be able to get a table")
        start = datetime.now()

        output = backend.get_table(table_name, dict_return)
        if output is not None and isinstance(output, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Get_Table() Error: {output[1]}")
            raise output[0](output[1])
        
        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")

        if output is not None and isinstance(output, (pd.DataFrame, OrderedDict)):
            return output

    def find(self, query_object):
        """
        Find all instances of `query_object` across all tables, columns, and cells in the first loaded backend.
       
        `query_object` : any
            The object to search for in the backend. Can be of any type, including str, float, or int.

        `return` : list
            A list of backend-specific result objects, each representing a match for `query_object`.
            The structure of each object depends on the backend implementation.

            - Refer to the first loaded backend's documentation to understand the structure of the objects in this list
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Finding `{query_object}` across all tables, columns, and cells in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing a find on it')
            raise NotImplementedError('Need to load a valid backend before performing a find on it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("Error in find all function: First loaded backend needs to have data to be able to find data from it")
            raise RuntimeError("Error in find all function: First loaded backend needs to have data to be able to find data from it")
        start = datetime.now()
        return_object = backend.find(query_object)
        return self.find_helper(query_object, return_object, start, "")
    
    def find_table(self, query_object):
        """   
        Find all tables whose name matches `query_object` in the first loaded backend.
       
        `query_object` : str
            The object to search for in the backend. HAS TO BE A str.

        `return` : list
            A list of backend-specific result objects, each representing a match for `query_object`.
            The structure of each object depends on the backend implementation.

            - Refer to the first loaded backend's documentation to understand the structure of the objects in this list
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Finding all tables whose name matches `{query_object}` in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing a find on it')
            raise NotImplementedError('Need to load a valid backend before performing a find on it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("Error in find table function: First loaded backend needs to have data to be able to find data from it")
            raise RuntimeError("Error in find table function: First loaded backend needs to have data to be able to find data from it")
        start = datetime.now()
        return_object = backend.find_table(query_object)
        return self.find_helper(query_object, return_object, start, "table ")
    
    def find_column(self, query_object, range = False):
        """
        Find all columns whose name matches `query_object` in the first loaded backend.
       
        `query_object` : str
            The object to search for in the backend. HAS TO BE A str.

        `range`: bool, optional, default False. 
            If True, then data-range of all numerical columns which match `query_object` is included in return

            If False, then data for each column that matches `query_object` is included in return

        `return` : list
            A list of backend-specific result objects, each representing a match for `query_object`.
            The structure of each object depends on the backend implementation.

            - Refer to the first loaded backend's documentation to understand the structure of the objects in this list
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Finding all columns whose name matches `{query_object}` in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing a find on it')
            raise NotImplementedError('Need to load a valid backend before performing a find on it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("Error in find column function: First loaded backend needs to have data to be able to find data from it")
            raise RuntimeError("Error in find column function: First loaded backend needs to have data to be able to find data from it")
        start = datetime.now()
        return_object = backend.find_column(query_object, range)
        return self.find_helper(query_object, return_object, start, "column ")

    def find_cell(self, query_object, row = False):
        """
        Find all cells that match the `query_object` in the first loaded backend.
       
        `query_object` : any
            The object to search for in the backend. Can be of any type, including str, float, or int.

        `row` : bool, default=False
            If True, includes the entire row of data for each matching cell in return.

            If False, includes only the value of the matching cell

        `return` : list
            A list of backend-specific result objects, each representing a match for `query_object`.
            The structure of each object depends on the backend implementation.

            - Refer to the first loaded backend's documentation to understand the structure of the objects in this list
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Finding all cells which match `{query_object}` in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing a find on it')
            raise NotImplementedError('Need to load a valid backend before performing a find on it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to find data from it")
            raise RuntimeError("First loaded backend needs to have data to be able to find data from it")
        start = datetime.now()
        return_object = backend.find_cell(query_object, row)
        return self.find_helper(query_object, return_object, start, "cell ")

    # Internal function to return found objects or print errors.
    def find_helper(self, query_object, return_object, start, find_type):
        """
        **Users should not call this. Used internally.**
        
        Helper function to print/log information for all core find functions: find(), find_table(), find_column(), find_cell()
        """
        val = f"'{query_object}'" if isinstance(query_object, str) else query_object
        print(f"Finding all {find_type}matches of {val} in first backend loaded")
        if isinstance(return_object, str):
            if self.debug_level != 0:
                self.logger.warning(return_object)
            print("WARNING:", return_object)
            return_object = None
        elif isinstance(return_object, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Error finding data due to {return_object[1]}")
            raise return_object[0](return_object[1])        
        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")
        return return_object
    
    def find_relation(self, query_object):
        """   
        Finds all rows in the first table of the first loaded backend that satisfy a column-level condition.
        `query_object` must include a column, operator, and value to define a valid relational condition.
       
        `query_object` : str
            A relational expression combining column, operator, and value.
            Ex: "age > 4", "age < 4", "age >= 4", "age <= 4", "age = 4", "age == 4", "age != 4", "age (4, 8)".

        `return` : list
            A list of backend-specific result objects, each representing a row that satisfies the relation.
            The structure of each object depends on the backend implementation.

            - Refer to the first loaded backend's documentation to understand the structure of the objects in this list
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Finding all rows in the first table of the first loaded backend where {query_object}')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before performing a find on it')
            raise NotImplementedError('Need to load a valid backend before performing a find on it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to find data from it")
            raise RuntimeError("First loaded backend needs to have data to be able to find data from it")
        start = datetime.now()

        if not isinstance(query_object, str):
            raise TypeError("`query_object` must be a string")
        print(f"Finding all rows in the first table of the first loaded backend where {query_object}")
        query_object = query_object.replace("\\'", "'") if "\\'" in query_object else query_object
        query_object = query_object.replace('\\"', '"') if '\\"' in query_object else query_object

        def is_literal(s):
            return s.startswith("'") and s.endswith("'")
        def wrap_in_quotes(value):
            value = value[1:-1] if is_literal(value) else value
            return "'" + re.sub(r"(?<!')'(?!')", "''", value) + "'"

        operators = ['==', '!=', '>=', '<=', '=', '<', '>', '(']
        if not any(op in query_object for op in operators):
            raise ValueError("`query_object` is missing an operator to compare the column to a value.")
        result = self.manual_string_parsing(query_object)
        if len(result) == 1:
            raise ValueError("Could not identify the operator in `query_object`. The operator cannot be nested in double quotes")
        elif len(result) == 2:
            raise ValueError("Input must include a column, operator, and value. Operator cannot be enclosed in quotes.")
        elif len(result) == 3 and not is_literal(result[2]) and any(op in result[2] for op in operators):
            extra = "If matching value has an operator in it, make sure to wrap in single quotes."
            raise ValueError(f"Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]. {extra}")
        elif len(result) == 3 and result[2].startswith('"') and result[2].endswith('"'):
            raise ValueError(f"The value in the relational find() cannot be enclosed in double quotes")
        
        column_name = result[0]
        relation = result[1] + result[2]
        if "'" in column_name:
            raise ValueError("Cannot have a single quote as part of a column name")
        if "'" in result[2] and result[2].count("'") % 2 != 0:
            raise TypeError("Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s")
        elif len(result) == 3 and result[1] == '(':
            start_msg = f"When applying a range-based find on '{column_name}' using (),"
            if ',' not in result[2]:
                raise ValueError(f"{start_msg} values must be separated by a comma.")
            elif ')' != result[2][-1]:
                raise ValueError(f"{start_msg} it must end with closing parenthesis.")
            elif (result[2][result[2].rfind("'"):] if "'" in result[2] else result[2]).count(')') > 1:
                raise ValueError("Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]")
        
            values = result[2][:-1].strip()
            if values[0] == '"' or values[-1] == '"':
                raise ValueError("Neither value in the range-based find can be enclosed in double quotes. Only single quotes")
            if "'" not in values or ("'" in values and not is_literal(values)):
                if values.count(',') > 1 or ' ' in values.replace(', ', ','):
                    raise ValueError("Range-based finds require multi-word values to be enclosed in single quotes")
            values = re.sub(r"\s*,\s*(?=(?:[^']*'[^']*')*[^']*$)", ",", values)
            values = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", values)
            if '' in values or len(values) != 2:
                raise ValueError("There needs to be two values for the range find. Ex: (1,2)")
            relation = f"({wrap_in_quotes(values[0])},{wrap_in_quotes(values[1])})"
            if values[0] > values[1]:
               raise ValueError(f"Invalid input range: '{relation}'. The lower value must come first.")
        else:
            relation = f"{result[1]} {wrap_in_quotes(result[2])}"

        return_object = backend.find_relation(column_name, relation)
        if isinstance(return_object, str):
            if self.debug_level != 0:
                self.logger.warning(return_object)
            print("WARNING:", return_object)
            return_object = None
        elif isinstance(return_object, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Error finding rows due to {return_object[1]}")
            raise return_object[0](return_object[1])
        elif isinstance(return_object, list) and isinstance(return_object[0], str):
            err_msg = f"'{column_name}' appeared in more than one table. Can only do a conditional find if '{column_name}' is in one table"
            if self.debug_level != 0:
                self.logger.warning(err_msg)
            print(f"WARNING: {err_msg}")
            print("Try using `artifact_handler('query', query = )` to retrieve the matching rows for a specific table")
            print("These are recommended inputs for artifact_handler():")
            for cond_query in return_object:
                print(f" - {cond_query}")
            return_object = None
        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")
        return return_object

    def overwrite_table(self, table_name, collection, backup = False):
        """
        Overwrites specified table(s) in the first loaded backend with the provided Pandas DataFrame(s).

        If a relational schema has been previously loaded into the backend, it will be reapplied to the table.
        **Note:** This function permanently deletes the existing table and its data, before inserting the new data.

        `table_name` : str or list
            - If str, name of the table to overwrite in the backend.
            - If list, list of all tables to overwrite in the backend

        `collection` : pandas.DataFrame  or list of Pandas.DataFrames
            - If one item, a DataFrame containing the updated data will be written to the table.
            - If a list, all DataFrames with updated data will be written to their own table
        
        `backup` : bool, optional, default False.
            - If True, creates a backup file for the DSI backend before updating its data.
            - If False, (default), only updates the data.
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Overwriting data in the table(s): {table_name} in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend to be able to overwrite a table')
            raise NotImplementedError('Need to load a valid backend to be able to overwrite a table')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to overwrite its data")
            raise RuntimeError("First loaded backend needs to have data to be able to overwrite its data")
        start = datetime.now()

        list_names = isinstance(table_name, list) and all(isinstance(name, str) for name in table_name)
        if not isinstance(table_name, str) and list_names == False:
            if self.debug_level != 0:
                self.logger.error("Input 'table_name' must be either a single table name or a list of table names")
            raise RuntimeError("Input 'table_name' must be either a single table name or a list of table names")
        if isinstance(table_name, str) and table_name.lower() in self.dsi_tables:
            if self.debug_level != 0:
                self.logger.error("Input 'table_name' cannot be a DSI-reserved table name. Try again.")
            raise RuntimeError("Input 'table_name' cannot be a DSI-reserved table name. Try again.")
        elif isinstance(table_name, list) and bool(set(tn.lower() for tn in table_name) & set(self.dsi_tables)):
            if self.debug_level != 0:
                self.logger.error("Input list of 'table_name' cannot include a DSI-reserved table name. Try again.")
            raise RuntimeError("Input list of 'table_name' cannot include a DSI-reserved table name. Try again.")

        list_dfs = isinstance(collection, list) and all(isinstance(df, pd.DataFrame) for df in collection)
        if not isinstance(collection, pd.DataFrame) and list_dfs == False:
            if self.debug_level != 0:
                self.logger.error("Input 'collection' must be either a single DataFrame or a list of DataFrames")
            raise RuntimeError("Input 'collection' must be either a single DataFrame or a list of DataFrames")

        if backup == True:
            if self.debug_level != 0:
                self.logger.info(f"   Creating backup file before overwriting data in the {backend.__class__.__name__} backend")
            backup_start = datetime.now()
            extension = backend.filename.rfind('.')
            backup_file = backend.filename[:extension] + ".backup" + backend.filename[extension:]
            shutil.copyfile(backend.filename, backup_file)
            backup_end = datetime.now()
            if self.debug_level != 0:
                self.logger.info(f"   Backup file creation runtime: {backup_end-backup_start}")

        errorStmt = backend.overwrite_table(table_name, collection)
        if errorStmt is not None and isinstance(errorStmt, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Overwrite_table() error: {errorStmt[1]}")
            raise errorStmt[0](errorStmt[1])
        
        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")
    
    def list(self):
        """
        Prints a list of all tables and their dimensions in the first loaded backend
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Listing data of all tables and their dimensions in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before listing all tables in it')
            raise NotImplementedError('Need to load a valid backend before listing all tables in it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to list its data")
            raise RuntimeError("First loaded backend needs to have data to be able to list its datd")
        start = datetime.now()
        
        table_list = backend.list()
        for table in table_list:
            print(f"\nTable: {table[0]}")
            print(f"  - num of columns: {table[1]}")
            print(f"  - num of rows: {table[2]}")
        print()

        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")

    def summary(self, table_name = None, collection = False):
        """
        Returns/Prints numerical metadata from tables in the first loaded backend.

        `table_name` : str, optional
            If specified, only the numerical metadata for that table will be returned/printed.

            If None (default), metadata for all available tables is returned/printed.
        
        `collection` : bool, optional, default False.
            - If True, returns either a list of DataFrames (table_name = None), or a single DataFrame of metadata
            - If False (default), prints metadata from all tables (table_name = None), or just a single table
        """
        if self.debug_level != 0 and table_name == None:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Summarizing numerical data of all tables in the first loaded backend')
        elif self.debug_level != 0 and table_name != None:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Summarizing numerical data of the table: {table_name} in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before printing table info from it')
            raise NotImplementedError('Need to load a valid backend before printing table info from it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to summarize its data")
            raise RuntimeError("First loaded backend needs to have data to be able to summarize its data")
        start = datetime.now()

        output = backend.summary(table_name)
        if output is not None and isinstance(output, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Summary error: {output[1]}")
            raise output[0](output[1])

        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")
        
        if collection == True and table_name is None:
            return output[1:]
        elif collection == True and table_name is not None:
            return output
        elif table_name is not None and isinstance(output, pd.DataFrame):
            print(f"\nTable: {table_name}")
            headers = output.columns.tolist()
            rows = output.values.tolist()
            self.table_print_helper(headers, rows, len(rows), 100)
        elif isinstance(output, list) and isinstance(output[0], list) and all(isinstance(df, pd.DataFrame) for df in output[1:]):
            for t_name, data in zip(output[0], output[1:]):
                print(f"\nTable: {t_name}")
                headers = data.columns.tolist()
                rows = data.values.tolist() 
                self.table_print_helper(headers, rows, len(rows), 100)
        else:
            raise ValueError("Returned object from the first loaded backend's summary() is incorrectly structured")

    def num_tables(self):
        """
        Prints number of tables in the first loaded backend
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Printing number of tables in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before listing all tables in it')
            raise NotImplementedError('Need to load a valid backend before listing all tables in it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to get its number of tables")
            raise RuntimeError("First loaded backend needs to have data to be able to get its number of tables")
        start = datetime.now()

        backend.num_tables()

        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")

    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data from a specified table in the first loaded backend.
        
        `table_name` : str
            Name of the table to display.
         
        `num_rows` : int, optional, default=25
            Number of rows to print. If the table contains fewer rows, only those are shown.

        `display_cols` : list of str, optional
            List of specific column names to display from the table. 

            If None (default), all columns are displayed.
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Displaying data from the table {table_name} in the first loaded backend')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend before printing table info from it')
            raise NotImplementedError('Need to load a valid backend before printing table info from it')
        backend = self.loaded_backends[0]
        parent_backend = backend.__class__.__bases__[0].__name__
        if not self.valid_backend(backend, parent_backend):
            if self.debug_level != 0:
                self.logger.error("First loaded backend needs to have data to be able to display its data")
            raise RuntimeError("First loaded backend needs to have data to be able to display its data")
        start = datetime.now()

        if not isinstance(table_name, str):
            raise TypeError("Input 'table_name' must be a string")
        if not isinstance(num_rows, int):
            raise TypeError("Input 'num_rows' must be a integer")
        if display_cols is not None and not isinstance(display_cols, list):
            raise TypeError("Input 'display_cols' must be a list of string column names")

        output = backend.display(table_name, num_rows, display_cols)
        if output is not None and isinstance(output, tuple):
            if self.debug_level != 0:
                self.logger.error(f"Display error: {output[1]}")
            raise output[0](output[1])

        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")
        
        if table_name is not None and isinstance(output, pd.DataFrame):
            max_rows = output.attrs["max_rows"]
            print(f"\nTable: {table_name}")
            headers = output.columns.tolist()
            rows = output.values.tolist()
            self.table_print_helper(headers, rows, max_rows, num_rows)
            print()
    
    def get_table_names(self, query):
        """
        Extracts and returns all table names referenced in a given query.

        `query` : str
            A query string written in a database language (typically SQL).
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.error(f'Getting all table names from the query: {query}')
        if len(self.loaded_backends) == 0:
            if self.debug_level != 0:
                self.logger.error('Need to load a valid backend to be able to identify table names in a query for that backend')
            raise NotImplementedError('Need to load a valid backend to be able to identify table names in a query for that backend')
        backend = self.loaded_backends[0]
        start = datetime.now()

        output = backend.get_table_names(query)
        if output is not None and isinstance(output, tuple):
            if self.debug_level != 0:
                self.logger.info(f"Error getting table names {output[1]}")
            raise output[0](output[1])
        
        end = datetime.now()
        if self.debug_level != 0:
            self.logger.info(f"Runtime: {end-start}")

        if output is not None and isinstance(output, list):
            return output
    
    def get_current_abstraction(self, table_name = None):
        """
        Returns the current DSI abstraction as a nested Ordered Dict.

        The abstraction is organized such that:
            - The outer OrderedDict has table names as keys.
            - Each value is an inner OrderedDict representing a table, where keys are column names and values are lists of column data.

        `table_name` : str, optional, default is None.
            If specified, returns only the OrderedDict corresponding to that table.

            If None (default), returns the full nested OrderedDict containing all tables.

        `return` : OrderedDict
            If `table_name` is None: returns a nested OrderedDict of all tables.
            
            If `table_name` is provided: returns a single OrderedDict for that table.
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
        Updates the DSI abstraction, by creating/overwriting the specified table_name with the input table_data

        `table_name`: str
            Name of the table to update/create.

        `table_data` : OrderedDict or Pandas DataFrame
            The new data to store in the table. If it is an Ordered Dict:

                - Keys are column names.
                - Values are lists representing column data.
        """
        if self.debug_level != 0:
            self.logger.info("-------------------------------------")
            self.logger.info(f"Updating abstraction table {table_name} with new data")
        if not isinstance(table_data, (OrderedDict, pd.DataFrame)):
            if self.debug_level != 0:
                self.logger.error("table_data needs to be in the form of an Ordered Dictionary or Pandas DataFrame")
            raise TypeError("table_data needs to be in the form of an Ordered Dictionary or Pandas DataFrame")
        if isinstance(table_data, OrderedDict):
            if not all(isinstance(val, list) for val in table_data.values()):
                if self.debug_level != 0:
                    self.logger.error("Each key of the Ordered Dict must be a column name and its value a list of row data")
                raise TypeError("Each key of the Ordered Dict must be a column name and its value a list of row data")
            self.active_metadata[table_name] = table_data
        elif isinstance(table_data, pd.DataFrame):
            self.active_metadata[table_name] = OrderedDict(table_data.to_dict(orient='list'))
    
    def close(self):
        """
        Immediately closes all active modules: backends, plugin writers, plugin readers

        Clears out the current DSI abstraction

        NOTE - This step cannot be undone.
        """
        print("Closing the abstraction layer, and all active readers/writers/backends")
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
    
    # Internal function to return input string as either int, float or still a string
    def check_type(self, text):
        try:
            _ = int(text)
            return int(text)
        except ValueError:
            try:
                _ = float(text)
                return float(text)
            except ValueError:
                return text

    # helper function to manually parse relation input to see if its valid
    def manual_string_parsing(self, query):
        # splits on double quotes not within any single quotes
        # splits on operators outside of any quotes
        op_pattern = re.compile(r'==|!=|>=|<=|=|<|>|\(')
        parts = []
        buffer = ''
        in_single = False
        in_double = False
        operator_found = False
        i = 0

        while i < len(query):
            char = query[i]
            if char == "'" and not in_double:
                in_single = not in_single
                buffer += char
                i += 1
            elif char == '"' and not in_single:
                in_double = not in_double
                buffer += char
                i += 1
            elif not in_single and not in_double:
                op_match = op_pattern.match(query, i)
                if op_match and not operator_found:
                    if buffer.strip():
                        parts.append(buffer.strip())
                    parts.append(op_match.group())
                    buffer = ''
                    operator_found = True
                    i += len(op_match.group())
                else:
                    buffer += char
                    i += 1
            else:
                buffer += char
                i += 1

        if buffer.strip():
            parts.append(buffer.strip())

        return parts

    # Internal function used to manually print a table cleanly
    def table_print_helper(self, headers, rows, max_rows, num_rows=25):
        # Determine max width for each column
        col_widths = [
            max(
                len(str(h)),
                max((len(str(r[i])) for r in rows if i < len(r)), default=0)
            )
            for i, h in enumerate(headers)
        ]

        # Print header
        header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
        print("\n" + header_row)
        print("-" * len(header_row))

        # Print each row
        count = 0
        for row in rows:
            print(" | ".join(
                f"{str(row[i]):<{col_widths[i]}}" for i in range(len(headers)) if i < len(row)
            ))

            count += 1
            if count == num_rows:
                print(f"  ... showing {num_rows} of {max_rows} rows")
                break

    # Internal function used to get line numbers from return statements - SHOULD NOT be called by users
    def trace_function(self, frame, event, arg):
        global return_line_number
        global original_file
        if event == "return":
            return_line_number = frame.f_lineno  # Get line number
            original_file = frame.f_code.co_filename # Get file name
        return self.trace_function

    # Internal function used to check if a backend has data
    def valid_backend(self, backend, parent_name):
        valid = False
        if parent_name == "Filesystem":
            if backend.__class__.__name__ == "Sqlite" and os.path.getsize(backend.filename) > 100:
                valid = True
            if backend.__class__.__name__ == "DuckDB" and os.path.getsize(backend.filename) > 13000:
                valid = True
            if backend.__class__.__name__ == "Parquet" and os.path.getsize(backend.filename) > 100:
                valid = True
        return valid


class Sync():
    """
    A class defined to assist in data management activities for DSI

    Sync is where data movement functions such as copy (to remote location) and 
    sync (local filesystem with remote) exist.
    """
    remote_location = []
    local_location = []

    file_list = []
    rfile_list = []

    def __init__(self, project_name="test"):
        # Helper function to get parent module names.
        #self.remote_location = {}
        #self.local_location = {}
        self.project_name = project_name

    def index(self, local_loc, remote_loc, isVerbose=False):
        """
        Helper function to gather filesystem information, local and remote locations
        to create a filesystem entry in a new or existing database
        """
        if isVerbose:
            print("loc: "+local_loc+ " rem: "+remote_loc)
        # Data Crawl and gather metadata of local location
        file_list = self.dircrawl(local_loc)

        self.remote_location = remote_loc
        self.local_location = local_loc
        # populate st_list to hold all filesystem attributes
        st_list = []
        rfile_list = []

        # Do a quick validation of group access permissions
        # While crawling os.stat info
        # Create ordered dictionary
        st_dict = OrderedDict()
        st_dict['file_origin'] = []
        st_dict['size']= []
        st_dict['modified_time'] = []
        st_dict['created_time'] = []
        st_dict['accessed_time'] = []
        st_dict['mode'] = []
        st_dict['inode'] = []
        st_dict['device'] = []
        st_dict['n_links'] = []
        st_dict['uid'] = []
        st_dict['gid'] = []
        st_dict['file_remote'] = []

        for file in file_list:
            rel_file = os.path.relpath(file,local_loc) #rel path
            #utils.isgroupreadable(file) # quick test
            filepath = os.path.join(local_loc, rel_file)
            st = os.stat(filepath)
            # append future location to st
            rfilepath = os.path.join(remote_loc,self.project_name, rel_file)
            rfile_list.append(rfilepath)
            st_dict['file_origin'].append(rel_file)
            st_dict['size'].append(st.st_size)
            st_dict['modified_time'].append(st.st_mtime)
            st_dict['created_time'].append(st.st_ctime)
            st_dict['accessed_time'].append(st.st_atime)
            st_dict['mode'].append(st.st_mode)
            st_dict['inode'].append(st.st_ino)
            st_dict['device'].append(st.st_dev)
            st_dict['n_links'].append(st.st_nlink)
            st_dict['uid'].append(st.st_uid)
            st_dict['gid'].append(st.st_gid)
            st_dict['file_remote'].append(rfilepath)
            st_list.append(st)


        # Test remote location validity, try to check access
        # Future: iterate through remote/server list here, for now:::
        remote_list = [ os.path.join(remote_loc,self.project_name) ]
        for remote in remote_list:
            try: # Try for file permissions
                if os.path.exists(remote): # Check if exists
                    print(f"The directory '{remote}' already exists remotely.")
                else:
                    os.makedirs(remote) # Create it
                    print(f"The directory '{remote}' has been created remotely.")
            except Exception as err:
                print(f"Unexpected {err=}, {type(err)=}")
                raise

        
        # Try to open existing local database to store filesystem info before copy
        # Open and validate local DSI data store
        t = Terminal()

        try:
            #f = os.path.join((local_loc, str(self.project_name+".db") ))
            #f = local_loc+"/"+self.project_name+".db"
            f = self.project_name+".db"
            if isVerbose:
                print("trying db: ", f)
            if os.path.exists(f):
                t.load_module('backend','Sqlite','back-write', filename=f)
            
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

        # See if filesystem exists
        try:
            fs_t = t.get_table("filesystem")
            if isVerbose:
                print("fs table exists already, skipping for now")
        except:
            if isVerbose:
                print( "Creating new fs table")
            # Create new filesystem collection with origin and remote locations
            # Stage data for ingest
            # Transpose the OrderedDict to a list of row dictionaries
            num_rows = len(next(iter(st_dict.values())))  # Assume all columns are of equal length
            rows = []

            for i in range(num_rows):
                row = {col: values[i] for col, values in st_dict.items()}
                rows.append(row)

            # Temporary csv to ingest
            output_file = '.fs.csv'
            with open(output_file, mode='w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=st_dict.keys())
                writer.writeheader()
                writer.writerows(rows)
            
            # Add filesystem table
            t.load_module('plugin', 'Csv', 'reader', filenames=".fs.csv", table_name="filesystem")
            t.artifact_handler(interaction_type='ingest')

        self.file_list = file_list
        self.rfile_list = rfile_list

    def move(self, tool="copy", isVerbose=False, **kwargs):
        self.copy(tool,isVerbose,kwargs)

    def copy(self, tool="copy", isVerbose=False, **kwargs):
        """
        Helper function to perform the 
        data copy over using a preferred API
        """
        # See if FS table has been created
        t = Terminal()

        try:
            #f = os.path.join((local_loc, str(self.project_name+".db") ))
            f = self.local_location+"/"+self.project_name+".db"
            if isVerbose:
                print("db: ", f)
            if os.path.exists(f):
                t.load_module('backend','Sqlite','back-read', filename=f)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

        # See if filesystem exists
        try:
            fs_t = t.get_table("filesystem")
        except:
            print( " Filesystem table not found. Try running Index first.")
            print( " Data copy failed. ")
            return
        
        # Future: have movement service handle type (cp,scp,ftp,rsync,etc.)
        if tool == "copy":
            print(self.file_list)
            print(self.rfile_list)
            # Data movement via Unix Copy
            for file,file_remote in zip(self.file_list,self.rfile_list):
                abspath = os.path.dirname(os.path.abspath(file_remote))
                if not os.path.exists(abspath):
                    if isVerbose:
                        print( " mkdir " + abspath)
                    path = Path(abspath)
                    path.mkdir(parents=True)

                if isVerbose:
                    print( " cp " + file + " " + file_remote)
                shutil.copy2(file , file_remote)
                

            # Database movement
            if isVerbose:
                print( " cp " + os.path.join(self.local_location, str(self.project_name+".db") ) + " " + os.path.join(self.remote_location, self.project_name, self.project_name+".db" ) )
            shutil.copy2(os.path.join(self.local_location, str(self.project_name+".db") ), os.path.join(self.remote_location, self.project_name, self.project_name+".db" ) )

            print( " Data Copy Complete! ")
        elif tool == "scp":
            #  Data movement via SCP
            remote_user = os.getlogin()
            remote_host = "myremote"

        elif tool == "ftp":
            True
        elif tool == "git":
            True
        else:
            raise TypeError(f"Data movement format not supported:, Type: {tool}")



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
    
    def get(self, project_name = "Project"):
        '''
        Helper function that searches remote location based on project name, and retrieves
        DSI database
        '''
        True