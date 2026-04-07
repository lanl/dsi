import os
import csv
import tarfile
import subprocess
import uuid
import yaml
import time
import itertools
import shutil
from pathlib import Path
from typing import Iterator
from contextlib import redirect_stdout
from collections import OrderedDict

from dsi.core import Terminal
from dsi.utils.federated import federate_datasets


from dsi.utils.federation_utils import (
    compute_md5, 
    create_directory, 
    create_folder_from_path, 
    csv_to_list_of_dicts, 
    deduplicate_keep_latest, 
    get_last_part, 
    human_readable_size, 
    should_download, 
    upsert_records
)

from dsi.utils.git_utils import download_github_file, get_github_remote_file_size
from dsi.utils.rsync_utils import rsync_download_interactive, ssh_remote_size_bytes_interactive
from dsi.utils.web_utils import download_web_file, get_url_file_size
from dsi.utils.federated.federate_datasets import federate_datasets

class Sync():
    """
    A class defined to assist in data management activities for DSI

    Sync is where data movement functions such as copy (to remote location) and
    sync (local filesystem with remote) exist.
    """
    def __init__(self, project_name=None):
        self.project_name = project_name
        extension = ""
        if project_name:
            for ext in (".duckdb", ".sqlite", ".db", ".sqlite3"):
                if project_name.lower().endswith(ext):
                    self.project_name = project_name[:-len(ext)]
                    extension = ext
                    break
            if extension != "":
                self.full_db_name = self.project_name + extension
            else:
                proj_db_found = False
                for ext in (".db", ".duckdb", ".sqlite", ".sqlite3"):
                    if os.path.exists(self.project_name + ext):
                        if proj_db_found:
                            raise ValueError(f"Multiple databases found with {project_name}. Specify an extension.")
                        self.full_db_name = self.project_name + ext
                        proj_db_found = True

            self.remote_location = None
            self.local_location = None
            self.file_list = None
            self.rfile_list = None

            self.t = Terminal()
            # first check if user can create db here
            if "/" in project_name:
                create_bool = self.t.can_create_file_here(project_name.rsplit("/", 1)[0])
            else:
                create_bool = self.t.can_create_file_here()
            if create_bool is False:
                raise RuntimeError(f"Cannot open the {project_name} database due to write permissions. Please try elsewhere.")
        
            backend_name = self.t.identify_backend(self.full_db_name)
            if backend_name is None:
                raise ValueError("Unsupported DSI database type. Currently supporting: Sqlite, DuckDB")

            fnull = open(os.devnull, 'w')
            with redirect_stdout(fnull):
                self.t.load_module('backend', backend_name, 'back-write', filename=self.full_db_name)

            if not self.t.valid_backend(self.t.loaded_backends[0], self.t.loaded_backends[0].__class__.__bases__[0].__name__):
                raise RuntimeError(f"{project_name} database must have metadata in it before trying to call DSI move functions.")

    def reindex(self, local_loc, remote_loc, isVerbose = False):
        """
        Helper function that allows users to index their data again by dropping existing filesystem information.
        """
        # current -- drop filesystem table and call index()
        # future --- use existing db to "reindex" by updating the filepath cols, not dropping table.
        #   remove filesystem pass through in query_artifacts if not dropping table
        self.t.artifact_handler(interaction_type='query', query = "DROP TABLE IF EXISTS filesystem;")
        self.index(local_loc, remote_loc, isVerbose)
    
    def index(self, local_loc, remote_loc, isVerbose=False, no_parent = False):
        """
        Helper function to gather filesystem information, local and remote locations
        to create a filesystem entry in a new or existing database
        """
        # throw error if filesystem exists -- in future, drop filesystem table if it exists.
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            fs_t = self.t.get_table("filesystem")
        if not fs_t.empty:
            if isVerbose:
                print("Warning: filesystem table already exists! DSI Index skipped.")
            return

        # Relative paths (..) will not work
        if "../" in local_loc or "../" in remote_loc:
            raise ValueError("Error: Please use absolute paths instead of relative")

        if isVerbose:
            print("loc: "+local_loc+ " rem: "+remote_loc)

        # Data Crawl and gather metadata of local location
        file_list = self.dircrawl2(local_loc, isVerbose)

        if isVerbose:
            file_list, tmp = itertools.tee(file_list)
            file_len=sum(1 for _ in tmp)
            print("Crawled "+str(file_len)+" files.")
        
        file_list = list(file_list) # save as list since dircrawl2() returns an iterator 
        
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
        st_dict['uuid'] = []
        st_dict['file_remote'] = []

        if isVerbose:
            print("Collection object [", end="")
            last = -10

        for file in file_list:
            parent_rel_file = Path(file).relative_to(Path(local_loc).parent)
            rel_file = os.path.relpath(file,local_loc) #rel path
            filepath = os.path.join(local_loc, rel_file)
            st = os.stat(filepath)
            # append future location to st
            if no_parent: # exclude parent dir of every file in remote location
                rfilepath = os.path.join(remote_loc,self.project_name, rel_file)
            else:
                rfilepath = os.path.join(remote_loc,self.project_name, parent_rel_file)
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
            st_dict['uuid'].append(self.gen_uuid(st))
            st_dict['file_remote'].append(rfilepath)
            st_list.append(st)
            if isVerbose:
                progress = int(len(st_list) / file_len * 100)
                # Print progress bar every 2%
                if progress % 2 == 0 and progress != last:
                    print(".", end="")
                    last = progress

        if isVerbose:
            print(f"] Collection object created with {len(st_list)} entries.")
                
        if isVerbose:
            print("Creating filesystem table")
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            self.t.load_module('plugin', "Dictionary", "reader", collection=st_dict, table_name="filesystem")
            self.t.artifact_handler(interaction_type='ingest')

        self.file_list = file_list
        self.rfile_list = rfile_list

        if isVerbose:
            print("DSI Index complete!\n")

    def move(self, tool="copy", isVerbose=False, **kwargs):
        self.copy(tool,isVerbose,kwargs)

    def execute_cmd(self, cmd, cmd_name, timer = False):
        """Internal helper for Sync to call executable actions"""
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='latin-1')

        if timer:
            start = time.time()
            while process.poll() is None:
                elapsed = int(time.time() - start)
                print(f"\rRunning... {elapsed}s elapsed", end="", flush=True)
                time.sleep(10)

        stdout, stderr = process.communicate()
        if process.returncode != 0:
            if "too many authentication failures" in str(stderr).lower():
                raise RuntimeError(f"{cmd_name} failed due to multiple incorrect password attempts. Check the password and remote path.")
            raise RuntimeError(f"{cmd_name} failed: \n{stderr}")
        return stdout
    
    def change_group(self, local_loc, user_group):
        """Change group permissions for data and db. Only works for OS with Unix (not Windows)"""
        try:
            cmd = ["chgrp", "-R", user_group, local_loc]
            self.execute_cmd(cmd, "changing user group for data", True)

            cmd = ["chgrp", user_group, self.full_db_name]
            self.execute_cmd(cmd, "changing user group for database")
        except Exception as e:
            print("Warning:", str(e))

    def change_permissions(self, local_loc):
        """Change read permissions for data and db. Only works for OS with Unix (not Windows)"""
        try:
            cmd = ["chmod", "-R", "750", local_loc] # 770 to make read/write to all. 750 to make read to all
            self.execute_cmd(cmd, "changing read permissions for data", True)

            cmd = ["chmod", "750", self.full_db_name]
            self.execute_cmd(cmd, "changing read permissions for database")
        except Exception as e:
            print("Warning:", str(e))
    
    def copy(self, tool="copy", isVerbose=False, **kwargs):
        """
        Helper function to perform the data copy over using a preferred API
        """
        if any(x is None for x in (self.remote_location, self.local_location, self.file_list, self.rfile_list)):
            raise RuntimeError("Must run successful DSI Index right before Copy")

        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            fs_t = self.t.get_table("filesystem")
        if fs_t.empty:
            print(" filesystem table not found. Must run Index first.")
            print(" Data copy failed.")
            return
        
        # Test remote location validity and try creating folders
        # Future: iterate through remote/server list here, for now:
        if tool.lower() not in ["scp", "rsync"]: # Exclude scp and rsync since they create folders differently
            remote_list = [ os.path.join(self.remote_location,self.project_name) ]
            for remote in remote_list:
                if isVerbose:
                    print(f"Testing access to '{remote}' directory.")
                try: # Try for file permissions
                    if os.path.exists(remote): # Check if exists
                        print(f"The directory '{remote}' already exists remotely.")
                    else:
                        path = Path(remote)
                        path.mkdir(parents=True, exist_ok=True)
                        # os.makedirs(remote) # Create it
                        print(f"The directory '{remote}' has been created remotely.")
                except Exception as err:
                    raise RuntimeError(f"Error creating remote directory: {err}")
        
        # Future: have movement service handle type without user input (cp,scp,ftp,rsync,etc.)
        if tool.lower() == "copy":
            # Data movement via Unix Copy
            for file,file_remote in zip(self.file_list,self.rfile_list):
                abspath = os.path.dirname(os.path.abspath(file_remote))
                if not os.path.exists(abspath):
                    if isVerbose:
                        print(" mkdir " + abspath)
                    path = Path(abspath)
                    try:
                        path.mkdir(parents=True)
                    except Exception:
                        raise RuntimeError(f"Unable to create folder {abspath}. Check your access rights")

                if isVerbose:
                    print(" cp " + file + " " + file_remote)
                shutil.copy2(file , file_remote)

            # Database movement
            if isVerbose:
                print(" cp " + self.full_db_name + " " + os.path.join(self.remote_location, self.project_name, self.full_db_name))
            shutil.copy2(self.full_db_name, os.path.join(self.remote_location, self.project_name, self.full_db_name))

            print(" Data Copy Complete!")
        
        elif tool.lower() == "scp":
            try:
                host_part, path_part = self.remote_location.split(":", 1)
            except ValueError:
                raise ValueError("Remote path must be in the format user@host:/absolute/path")

            if not path_part.startswith("/") and "nt" not in os.name:
                raise ValueError("Remote path must be absolute (starting with /)")
            
            # making remote dir
            if isVerbose:
                print(" ssh "+ str(host_part) + " \"mkdir -p " + str(os.path.join(path_part, self.project_name)) + "\"" )
            cmd = ["ssh", host_part, f'mkdir -p \"{os.path.join(path_part, self.project_name)}\"']
            print("Creating remote directory if it doesn't exist")
            self.execute_cmd(cmd, "Creating remote dir")


            #remove username from file_remote column in filesystem table
            username, host = host_part.split("@")
            filesystem_df = self.t.get_table("filesystem")
            filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(f"{username}@", "", regex=False)

            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table("filesystem", filesystem_df)
            self.t.dsi_tables.append("filesystem")

            cmd = ["scp", "-rp", self.local_location, os.path.join(self.remote_location, self.project_name)]
            if isVerbose:
                print()
                print(*cmd)
            self.execute_cmd(cmd, "scp data")
            print(" DSI SCP data movement complete.")

            cmd = ["scp", "-p", self.full_db_name, os.path.join(self.remote_location, self.project_name, self.full_db_name)]
            if isVerbose:
                print()
                print(*cmd)
            self.execute_cmd(cmd, "scp database")
            print(" DSI SCP database movement complete.")
        
        elif tool.lower() == "rsync":
            try:
                host_part, path_part = self.remote_location.split(":", 1)
            except Exception:
                raise ValueError("Remote location must be in the format user@host:/absolute/path")

            if not path_part.startswith("/"):
                raise ValueError("Remote path must be absolute (starting with /)")
            
            #remove username from file_remote column in filesystem table
            try:
                username, host = host_part.split("@")
            except Exception:
                raise ValueError("Remote path's hostname must be in the format user@server") from None
            filesystem_df = self.t.get_table("filesystem")
            filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(f"{username}@", "", regex=False)

            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table("filesystem", filesystem_df)
            self.t.dsi_tables.append("filesystem")
            
            self.local_location = self.local_location[:-1] if self.local_location.endswith("/") else self.local_location
            cmd = ["rsync", "-av", f"--rsync-path=mkdir -p {os.path.join(path_part, self.project_name)} && rsync", 
                   self.local_location, os.path.join(self.remote_location, self.project_name)]
            if isVerbose:
                print(*cmd)
            self.execute_cmd(cmd, "rsync data")
            print(" DSI Rsync data movement complete.")
            
            cmd = ["rsync", "-av", self.full_db_name, os.path.join(self.remote_location, self.project_name)]
            if isVerbose:
                print()
                print(*cmd)
            self.execute_cmd(cmd, "rsync database")
            print(" DSI Rsync database movement comlpete.")
        
        elif tool.lower() == "conduit":
            import signal

            # Test Kerberos
            if isVerbose:
                print( "Testing: klist")
            cmd = ['klist']
            stdout = self.execute_cmd(cmd, "Testing klist")
            if "No credentials" in stdout:
                print("Kerberos authentication error: No credentials found. Please type 'conduit get' to reissue a ticket.")
                raise RuntimeError("Kerberos message: " + str(stdout))

            # Test Conduit status
            def alarm_handler(signum, frame):
                raise RuntimeError("Conduit not authenticated. Please type 'conduit get' to issue a ticket.")
            signal.signal(signal.SIGALRM, alarm_handler)
            signal.alarm(10)

            try:
                if isVerbose:
                    print("Testing Conduit: conduit get")
                cmd = ['/usr/projects/systems/conduit/bin/conduit-cmd','--config','/usr/projects/systems/conduit/conf/conduit-cmd-config.yaml','get']
                stdout = self.execute_cmd(cmd, "Testing conduit get")

                if "TRANSFER_ID" in stdout and isVerbose:
                    print(" Conduit is authenticated.")
                elif "TRANSFER_ID" not in stdout:
                    raise RuntimeError("Conduit Error: " + str(stdout))
            finally:
                signal.alarm(0)

            try:
                base_cmd = ['/usr/projects/systems/conduit/bin/conduit-cmd','--config','/usr/projects/systems/conduit/conf/conduit-cmd-config.yaml','cp','-r']
                # File Movement
                if isVerbose:
                    print("conduit cp -r " + self.local_location + " " + os.path.join(self.remote_location, self.project_name))
                cmd = base_cmd + [self.local_location, os.path.join(self.remote_location, self.project_name)]
                self.execute_cmd(cmd, "Conduit copy data")
                print(" DSI submitted Conduit data movement job.")

                # Database Movement
                if isVerbose:
                    print("conduit cp " + self.full_db_name + " " + os.path.join(self.remote_location, self.project_name, self.full_db_name))
                cmd = base_cmd + [self.full_db_name, os.path.join(self.remote_location, self.project_name, self.full_db_name)]
                self.execute_cmd(cmd, "Conduit copy database")
                print(" DSI submitted Conduit database movement job.")

                print("Type 'conduit get' to track status of both jobs.")
                print("  If 'WaitingForLease' status, data move is in queue.")
                print("  If 'Error' status, type 'conduit error <TRANSFER_ID>' to view detailed error output.")

            except Exception as e:
                raise RuntimeError(f"Conduit failed with error: {str(e)} ")

        elif tool.lower() == "pfcp":           
            try:
                # File Movement
                if isVerbose:
                    print("pfcp -R " + self.local_location + " " + os.path.join(self.remote_location, self.project_name))
                cmd = ['pfcp','-R',self.local_location,  os.path.join(self.remote_location, self.project_name)]
                self.execute_cmd(cmd, "pfcp move data")
                print(" DSI submitted pfcp data movement job.")

                # Database Movement
                if isVerbose:
                    print("pfcp " + self.full_db_name + " " + os.path.join(self.remote_location, self.project_name, self.full_db_name))
                cmd = ['pfcp', self.full_db_name, os.path.join(self.remote_location, self.project_name, self.full_db_name)]
                self.execute_cmd(cmd, "pfcp move database")
                print(" DSI submitted pfcp database movement job.")
            except Exception as e:
                raise RuntimeError(f"pfcp failed with error: {str(e)} ")
        
        elif tool.lower() == "ftp":
            True
        elif tool.lower() == "git":
            True
        else:
            raise TypeError(f"Data movement format not supported:, Type: {tool}")


    def dircrawl(self,filepath, verbose=False):
        """
        Crawls the root 'filepath' directory and returns files

        `filepath`: source filepath to be crawled

        `return`: returns crawled file-list
        """
        start_time = time.perf_counter()

        file_list = []
        for root, dirs, files in os.walk(filepath):
            #if os.path.basename(filepath) != 'tmp': # Lets skip some files
            #    continue
            if verbose:
                print(f"Crawling directory: {root}")
                print(f"  Found {len(files)} files, {len(dirs)} subdirectories")

            for f in files: # Append root-level files
                file_list.append(os.path.join(root, f))

        elapsed = time.perf_counter() - start_time

        if verbose:
            print(f"\nFinished crawling: {filepath}")
            print(f"Total files found: {len(file_list)}")
            print(f"Runtime: {elapsed:.2f} seconds")

        return file_list

    def dircrawl2(self, filepath: str, verbose: bool = False) -> Iterator[str]:
        start = time.perf_counter()

        # iterative stack avoids deep recursion limits
        stack = [filepath]
        while stack:
            path = stack.pop()
            try:
                with os.scandir(path) as it:
                    for entry in it:
                        # follow_symlinks=False avoids surprises and extra stat calls
                        if entry.is_dir(follow_symlinks=False):
                            if verbose:
                                print(f"Crawling directory: {entry.path}")
                            stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            yield entry.path
            except (PermissionError, FileNotFoundError):
                # permissions/races happen a lot at scale
                continue

        if verbose:
            print(f"\nFinished crawling: {filepath}")
            print(f"Runtime: {time.perf_counter() - start:.2f} seconds")

    def get(self, input_yaml = None, workspace_folder= None):
        '''
        Helper function that searches remote location based input yaml file, and retrieves metadata that contains DSI databases
        '''

        # Read configuration from YAML file
        if input_yaml:
            try:
                yaml_path = Path(input_yaml)
                config_data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                print(f"Error: Could not find YAML file {yaml_path}")
        else:
            True
        
        # Create a folder for the databases if it doesn't exist, or use the provided one
        if not workspace_folder:
            _workspace_folder = config_data.get("workspace_folder", "")
            workspace_folder = _workspace_folder or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"


        federate_datasets(workspace_folder, config_data)

    def gen_uuid(self, st):
        '''
        Generates a unique file identifier using the os.stat data object as the input

        '''
        inode=st.st_ino
        ctime=st.st_ctime
        unique_str = f"{inode}-{ctime}"

        file_uuid = uuid.uuid5(uuid.NAMESPACE_URL, unique_str)
        #print(f"UUID:{file_uuid}")
        return str(file_uuid)



class TarFile():
  def __init__(self, tar_name, local_files, local_tmp_dir = 'tmp'):
    self.tar_name = tar_name
    self.local_tmp_dir = local_tmp_dir
    self.local_files = local_files
    self.create_tar(self.local_files)

  def create_tar(self, local_files=[]):
    """
    Creates a tar file and returns the index

    tar_name: name of the tar file to create with .tar.gz as the extension
    local_files: a list of files with full paths to include

    The tar file will be created in the local_tmp_dir directory
    """

    if not os.path.exists(self.local_tmp_dir):
        try:
            os.mkdir(self.local_tmp_dir)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")

    self.tar_path = self.local_tmp_dir + "/" + self.tar_name
    tar = tarfile.open(self.tar_path, "w:gz")
    for f in local_files:
        tar.add(f)
    tar.close()

    # Create an index. Taken from: https://stackoverflow.com/questions/2018512/reading-tar-file-contents-without-untarring-it-in-python-script
    tar = tarfile.open(self.tar_path)
    index = {i.name: i for i in tar.getmembers()}
    self.tar_index = ""
    for file_name in index.keys():
      self.tar_index += "%s : %d\n" % (file_name, index[file_name].size)

    return True

  def get_index(self):
    return self.tar_index

  def get_full_path(self):
      return self.tar_path

  def get_name(self):
      return self.tar_name

class HPSSSync():
    """
    A class defined to assist in HPSS data management activities for DSI

    Sync is where data movement functions such as copy (to HPSS) and
    sync (local filesystem with remote) exist.
    """
    remote_dir = None
    local_files = []
    tar_files = []

    def __init__(self, project_name="test"):
        # Helper function to get parent module names.

        self.project_name = project_name

    def index(self, local_files, remote_dir, tar_name, isVerbose=False):
        """
        Helper function to gather local file information and to create a tar that should
        be in remote_dir or will be copied to the remote directory

        local_files: a list of files or directories to add to a tar
        remote_dir: a directory on HPSS that has or will have the tar file
        """

        if isVerbose:
            print("loc: "+ str(local_files) + " hpss remote: "+remote_dir)

        # Tar the local_files list
        tar_file = TarFile(tar_name, local_files)
        self.local_files = local_files
        self.remote_dir = remote_dir
        self.tar_files.append(tar_file)
        st_list = []

        # Create ordered dictionary to store file information
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

        for tar_file in self.tar_files:
            st = os.stat(tar_file.get_full_path())
            # append future location to st
            st_dict['file_origin'].append(tar_file.get_full_path())
            st_dict['size'].append(st.st_size)
            st_dict['modified_time'].append(st.st_mtime)
            st_dict['created_time'].append(st.st_ctime)
            st_dict['accessed_time'].append(st.st_atime)
            st_dict['mode'].append(st.st_mode)
            st_dict['inode'].append(0)
            st_dict['device'].append(st.st_dev)
            st_dict['n_links'].append(st.st_nlink)
            st_dict['uid'].append(st.st_uid)
            st_dict['gid'].append(st.st_gid)
            st_dict['file_remote'].append(self.remote_dir + "/" + tar_file.get_name())
            st_list.append(st)

        # Try to open existing local database to store filesystem info before copy
        # Open and validate local DSI data store
        t = Terminal()

        f = self.project_name+".db"
        try:
            if isVerbose:
                print("trying db: ", f)
            assert os.path.exists(f)

            fnull = open(os.devnull, 'w')
            with redirect_stdout(fnull):
                t.load_module('backend','Sqlite','back-read', filename=f)
        except Exception:
            print(f"Database {f} not found")
            raise

        # See if filesystem exists
        fs_t = t.get_table("filesystem_hpss")
        if fs_t.empty:
            if isVerbose:
                print("Creating new hpss Filesystem table")
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
            t.load_module('plugin', 'Csv', 'reader', filenames=".fs.csv", table_name="filesystem_hpss")
            t.artifact_handler(interaction_type='ingest')

        t.close()

    def move(self, tool="copy", isVerbose=False, **kwargs):
        self.copy(tool,isVerbose,kwargs)

    def copy(self, tool="copy", isVerbose=False, **kwargs):
        """
        Helper function to perform the
        data copy over using a preferred API
        """
        # See if FS table has been created
        t = Terminal()

        f = self.project_name+".db"
        try:
            if isVerbose:
                print("trying db: ", f)
            assert os.path.exists(f)

            fnull = open(os.devnull, 'w')
            with redirect_stdout(fnull):
                t.load_module('backend','Sqlite','back-read', filename=f)
        except Exception:
            print(f"Database {f} not found")
            raise

        # See if filesystem exists
        fs_t = t.get_table("filesystem_hpss")
        if fs_t.empty:
            print( " Filesystem table not found. Try running Index first.")
            print( " Data copy failed. ")
            return

        t.close()

        hpss_files = {}
        for f in self.tar_files:
            hpss_files[self.remote_dir + "/" + f.get_name()] = f.get_full_path()
            if isVerbose:
                print( " copying " + f.get_full_path() + " to: " + self.remote_dir)

        t.load_module('backend','HPSS', 'back-write', hpss_files=hpss_files)
        # Data movement via the hsi HPSS commands
        t.artifact_handler(interaction_type='ingest')
        print( " Data Copy Complete! ")