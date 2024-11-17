from importlib import import_module
from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from itertools import product
import os
import shutil
from pathlib import Path
import logging
from datetime import datetime

from dsi.backends.filesystem import Filesystem
from dsi.backends.sqlite import Sqlite, DataType, Artifact


class Terminal():
    """
    An instantiated Terminal is the DSI human/machine interface.

    Terminals are a home for Plugins and an interface for Backends. Backends may be
    back-reads or back-writes. Plugins may be Writers or readers. See documentation
    for more information.
    """
    BACKEND_PREFIX = ['dsi.backends']
    BACKEND_IMPLEMENTATIONS = ['gufi', 'sqlite', 'parquet']
    PLUGIN_PREFIX = ['dsi.plugins']
    PLUGIN_IMPLEMENTATIONS = ['env', 'file_reader', 'file_writer']
    VALID_PLUGINS = ['Hostname', 'SystemKernel', 'GitInfo', 'Bueno', 'Csv', 'ER_Diagram', 'YAML1', 'TOML1', "Table_Plot", "Schema"]
    VALID_BACKENDS = ['Gufi', 'Sqlite', 'Parquet']
    VALID_MODULES = VALID_PLUGINS + VALID_BACKENDS
    VALID_MODULE_FUNCTIONS = {'plugin': ['writer', 'reader'], 
                              'backend': ['back-read', 'back-write']}
    VALID_ARTIFACT_INTERACTION_TYPES = ['get', 'set', 'put', 'inspect', 'read']

    def __init__(self, debug_flag = False, backup_db_flag = False):
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

        self.backup_db_flag = backup_db_flag

        self.logger = logging.getLogger(self.__class__.__name__)

        if debug_flag:
            logging.basicConfig(
                filename='logger.txt',         # Name of the log file
                filemode='w',               # Overwrite mode ('a' for append)
                format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
                level=logging.INFO          # Minimum log level to capture
            )

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
        self.logger.info(f"-------------------------------------")
        self.logger.info(f"Loading {mod_name} {mod_function} {mod_type}")
        start = datetime.now()
        if self.transload_lock and mod_type == 'plugin':
            print('Plugin module loading is prohibited after transload. No action taken.')
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
            return
        if mod_function not in self.VALID_MODULE_FUNCTIONS[mod_type]:
            print(
                'Hint: Did you declare your Module Function in the Terminal Global vars?')
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
            raise NotImplementedError
        if mod_name in [obj.__class__.__name__ for obj in self.active_modules[mod_function]]:
            print('{} {} already loaded as {}. Nothing to do.'.format(
                mod_name, mod_type, mod_function))
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
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
            end = datetime.now()
            self.logger.info(f"Runtime: {end-start}")
            raise NotImplementedError
        end = datetime.now()
        self.logger.info(f"Runtime: {end-start}")

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
                if mod_type == 'backend':
                    mod.close()
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
            (k, self.active_modules[k]) for k in ('reader', 'writer'))
        for module_type, objs in selected_function_modules.items():
            for obj in objs:
                self.logger.info(f"-------------------------------------")
                self.logger.info(obj.__class__.__name__ + f" {module_type}")
                if module_type == "reader":
                    start = datetime.now()
                    obj.add_rows(**kwargs)
                    for table_name, table_metadata in obj.output_collector.items():
                        if table_name not in self.active_metadata.keys():
                            self.active_metadata[table_name] = table_metadata
                        else:
                            for colName, colData in table_metadata.items():
                                if colName in self.active_metadata[table_name].keys() and table_name != "dsi_units":
                                    self.active_metadata[table_name][colName] += colData
                                elif colName not in self.active_metadata[table_name].keys() and table_name == "dsi_units":
                                    self.active_metadata[table_name][colName] = colData
                                elif colName not in self.active_metadata[table_name].keys() and table_name != "dsi_units":
                                    raise ValueError(f"Mismatched column input for table {table_name}")
                    end = datetime.now()
                    self.logger.info(f"Runtime: {end-start}")
                elif module_type == "writer":
                    start = datetime.now()
                    obj.get_rows(self.active_metadata, **kwargs)
                    end = datetime.now()
                    self.logger.info(f"Runtime: {end-start}")

        self.transload_lock = True

    def artifact_handler(self, interaction_type, query = None, **kwargs):
        """
        Store or retrieve using all loaded DSI Backends with storage functionality.

        A DSI Core Terminal may load zero or more Backends with storage functionality.
        Calling artifact_handler will execute all back-write functionality currently loaded, given
        the provided ``interaction_type``.
        """
        if interaction_type not in self.VALID_ARTIFACT_INTERACTION_TYPES:
            print('Hint: Did you declare your artifact interaction type in the Terminal Global vars?')
            raise NotImplementedError
        operation_success = False
        # Perform artifact movement first, because inspect implementation may rely on
        # self.active_metadata or some stored artifact.
        selected_active_backends = dict((k, self.active_modules[k]) for k in (['back-write', 'back-read']))
        for module_type, objs in selected_active_backends.items():
            for obj in objs:
                self.logger.info(f"-------------------------------------")
                self.logger.info(obj.__class__.__name__ + f" backend - {interaction_type} the data")
                start = datetime.now()
                if (interaction_type == 'put' or interaction_type == 'set') and module_type == 'back-write':
                    if self.backup_db_flag == True and os.path.getsize(obj.filename) > 100:
                        backup_file = obj.filename[:obj.filename.rfind('.')] + "_backup" + obj.filename[obj.filename.rfind('.'):]
                        shutil.copyfile(obj.filename, backup_file)
                    errorMessage = obj.put_artifacts(
                        collection=self.active_metadata, **kwargs)
                    if errorMessage is not None:
                        print(f"No data was inserted into {obj.filename} due to the error: {errorMessage}")
                    operation_success = True
                elif interaction_type == 'get':
                    self.logger.info(f"Query to get data: {query}")
                    if query != None:
                        self.active_metadata = obj.get_artifacts(query, **kwargs)
                    else:
                        #raise ValueError("Need to specify a query of the database to return data")
                        # This is a valid use-case, may give a warning for now
                        self.active_metadata = obj.get_artifacts(**kwargs)
                    operation_success = True
                elif interaction_type == 'inspect':
                    if module_type == 'back-write':
                        errorMessage = obj.put_artifacts(
                            collection=self.active_metadata, **kwargs)
                        if errorMessage is not None:
                            print("Error in ingesting data to db in inspect artifact. Generating Jupyter notebook with previous instance of db")
                    obj.inspect_artifacts(
                            collection=self.active_metadata, **kwargs)
                    operation_success = True
                elif interaction_type == "read" and module_type == 'back-read':
                    self.active_metadata = obj.read_to_artifact()
                    operation_success = True
                end = datetime.now()
                self.logger.info(f"Runtime: {end-start}")
        if operation_success:
            if interaction_type == 'get' and self.active_metadata:
                return self.active_metadata
            return
        else:
            print('Is your artifact interaction spelled correct and is it implemented in your backend?')
            print('Remember that backend writers cannot read a db and backend readers cannot write to a db')
            raise NotImplementedError

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
        
