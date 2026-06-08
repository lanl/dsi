import os
import csv
import tarfile
import subprocess
import uuid
import yaml
import time
import itertools
import shutil
import pandas as pd
from pathlib import Path
from typing import Iterator
from contextlib import redirect_stdout
from collections import OrderedDict

from dsi.core import Terminal
from dsi.utils.federated.federate_datasets import federate_datasets, pull_data

class Sync():
    """
    A class defined to assist in data management activities for DSI

    Sync is where data movement functions such as copy (to remote location) and
    sync (local filesystem with remote) exist.
    """
    def __init__(self, project_name, isVerbose = False, no_parent = False, skip_index = False, **kwargs):
        self.project_name = project_name
        self.verbose = isVerbose
        self.no_parent = no_parent
        self.skip_index = skip_index
        self.add_dbs = kwargs.pop("add_dbs", [])

        extension = ""
        for ext in (".duckdb", ".sqlite", ".db", ".sqlite3"):
            if project_name.lower().endswith(ext):
                self.project_name = project_name[:-len(ext)]
                extension = ext
                break
        if extension != "":
            self.full_db_name = self.project_name + extension
            if not os.path.exists(self.full_db_name):
                print("Creating new database: " + self.full_db_name)
                # We now allow a user to begin indexing from an empty database, so bypass raise
                #raise ValueError(f"Database {self.full_db_name} not found. Please input an existing database name.")

        else:
            proj_db_found = False
            for ext in (".db", ".duckdb", ".sqlite", ".sqlite3"):
                if os.path.exists(self.project_name + ext):
                    if proj_db_found:
                        raise ValueError(f"Multiple databases found with {project_name}. Specify an extension.")
                    self.full_db_name = self.project_name + ext
                    proj_db_found = True

            if not proj_db_found:
                raise ValueError(f"No database found with name {project_name}. Please input an existing database name.")
        
        self.remote_location = None
        self.local_location = None

        self.t = Terminal()
        # First check if user can create db here
        if "/" in project_name:
            create_bool = self.t.can_create_file_here(project_name.rsplit("/", 1)[0])
        else:
            create_bool = self.t.can_create_file_here()
        if create_bool is False:
            raise RuntimeError(f"Cannot open the {project_name} database due to write permissions. Please try elsewhere.")
    
        backend_name = self.t.identify_backend(self.full_db_name)
        # Allows an empty database to be created, autoselect SQLite for the user
        if backend_name is None:
            #raise ValueError("Unsupported DSI database type. Currently supporting: Sqlite, DuckDB")
            print("Auto-selecting sqlite backend.")
            backend_name = "Sqlite"

        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            self.t.load_module('backend', backend_name, 'back-write', filename=self.full_db_name)

        # Actually create a database and add a placeholder table
        if not self.t.valid_backend(self.t.loaded_backends[0]):
            #raise RuntimeError(f"{project_name} database must have metadata in it before trying to call DSI move functions.")
            with redirect_stdout(fnull):
                self.t.load_module('plugin', "Dictionary", "reader", collection={'location_type': ""}, table_name="federation")
                self.t.artifact_handler(interaction_type='ingest')


    def index(self, local_loc, remote_loc):
        """
        Helper function to gather filesystem information, local and remote locations
        to create a filesystem entry in a new or existing database
        """
        if "../" in local_loc or "../" in remote_loc:
            raise ValueError("Error: Please use absolute paths instead of relative")
        
        local_loc = local_loc if local_loc.endswith("/") else local_loc + "/"
        remote_loc = remote_loc if remote_loc.endswith("/") else remote_loc + "/"

        if self.verbose:
            print("loc: " + local_loc + " rem: " + remote_loc)

        table_list = self.t.list(True)
        if "federated" in table_list and "filesystem" in table_list:
            filesystem_df = self.t.get_table("filesystem")
            # only skip reindexing if data not moved yet
            if "file_abs" in filesystem_df.columns.tolist():
                fed_table = self.t.get_table("federated")
                fed_remote, fed_local = fed_table.loc[0, ["remote_location", "local_location"]]
                if fed_local == local_loc:
                    self.remote_location = os.path.join(remote_loc, self.project_name) + os.sep
                    self.local_location = local_loc
                    if fed_remote == remote_loc:
                        if self.verbose:
                            print("DSI Index complete!\n")
                        return
                    
                    # update remote file paths to use new remote location
                    filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(fed_remote, remote_loc, regex=False)
                    
                    # update remote location in federated table
                    fed_table.at[fed_table.index[0], "remote_location"] = os.path.join(remote_loc, self.project_name) + os.sep

                    self.t.dsi_tables.remove("filesystem")
                    self.t.overwrite_table(["federated", "filesystem"], [fed_table, filesystem_df])
                    self.t.dsi_tables.append("filesystem")

                    if self.verbose:
                        print("DSI Index complete!\n")
                    return
            else:
                if self.skip_index:
                    # adding fake file_abs col so it goes through actual skip dircrawl check above
                    filesystem_df["file_abs"] = None
                    self.t.dsi_tables.remove("filesystem")
                    self.t.overwrite_table("filesystem", filesystem_df)
                    self.t.dsi_tables.append("filesystem")
                    return self.index(local_loc, remote_loc)

        # Data Crawl and gather metadata of local location
        file_list = self.dircrawl2(local_loc, self.verbose)

        if self.verbose:
            file_list, tmp = itertools.tee(file_list)
            file_len=sum(1 for _ in tmp)
            print("Crawled "+str(file_len)+" files.")
        
        file_list = list(file_list) # save as list since dircrawl2() returns an iterator 
        
        self.remote_location = os.path.join(remote_loc, self.project_name) + os.sep
        self.local_location = local_loc
        # populate st_list to hold all filesystem attributes
        st_list = []

        # Do a quick validation of group access permissions
        # While crawling os.stat info
        # Create ordered dictionary
        st_dict = OrderedDict()
        st_dict['file_origin'] = []
        st_dict['file_abs'] = [] # Temporary column for unix copy
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

        if self.verbose:
            print("Collection object [", end="")
            last = -10

        for file in file_list:
            parent_rel_file = Path(file).relative_to(Path(local_loc).parent)
            rel_file = os.path.relpath(file,local_loc) #rel path
            filepath = os.path.join(local_loc, rel_file)
            st = os.stat(filepath)
            # append future location to st
            if self.no_parent: # exclude parent dir of every file in remote location
                rfilepath = os.path.join(remote_loc, self.project_name, rel_file)
            else:
                rfilepath = os.path.join(remote_loc, self.project_name, parent_rel_file)
            st_dict['file_origin'].append(rel_file)
            st_dict['file_abs'].append(file) # Temporary column for unix copy
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
            if self.verbose:
                progress = int(len(st_list) / file_len * 100)
                # Print progress bar every 2%
                if progress % 2 == 0 and progress != last:
                    print(".", end="")
                    last = progress

        if self.verbose:
            print(f"] Collection object created with {len(st_list)} entries.")
                
        if self.verbose:
            print("Creating filesystem table")
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            if "filesystem" in table_list:
                new_fs_df = pd.DataFrame(st_dict)
                self.t.dsi_tables.remove("filesystem")
                self.t.overwrite_table("filesystem", new_fs_df)
                self.t.dsi_tables.append("filesystem")
            else:
                self.t.load_module('plugin', "Dictionary", "reader", collection=st_dict, table_name="filesystem")

            # creating federated table -- 2 columns: local location and remote location
            fed_dict = {"local_location": [self.local_location], "remote_location": [self.remote_location]}
            if "federated" in table_list:
                df = pd.DataFrame(fed_dict)
                self.t.overwrite_table("federated", df)
            else:
                self.t.load_module('plugin', "Dictionary", "reader", collection=fed_dict, table_name="federated")              
            
            self.t.artifact_handler(interaction_type='ingest')

        if self.verbose:
            print("DSI Index complete!\n")


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
                raise RuntimeError(f"{cmd_name} failed due to multiple incorrect password attempts. Check the password and remote path.") from None
            elif "No credentials" in stderr:
                raise RuntimeError("Kerberos authentication error: No credentials found. Please type 'conduit get' to issue a ticket.\n"
                                   f"Kerberos message: {str(stderr)}") from None
            raise RuntimeError(f"{cmd_name} failed: \n{stderr}") from None
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


    def move(self, tool="copy"):
        self.copy(tool)


    def copy(self, tool="copy"):
        """
        Helper function to perform the data copy over using a preferred API
        """
        if any(x is None for x in (self.remote_location, self.local_location)):
            raise RuntimeError("Must successfully run DSI Index before Copy")

        # move additional dbs as well if specified in init
        db_list = [self.full_db_name]
        db_list.extend(self.add_dbs)

        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            filesystem_df = self.t.get_table("filesystem")
        if filesystem_df.empty:
            print(" filesystem table not found. Must run Index first.")
            print(" Data copy failed.")
            return
        
        file_list = filesystem_df["file_abs"].tolist()
        rfile_list = filesystem_df["file_remote"].tolist()
        
        # Test remote location validity and try creating folders
        # TODO: iterate through remote/server list here, for now:
        if tool.lower() not in ["scp", "rsync"]: # Exclude scp and rsync since they create folders differently
            remote_list = [self.remote_location]
            for remote in remote_list:
                if self.verbose:
                    print(f"Testing access to '{remote}' directory.")
                try: # Try for file permissions
                    if os.path.exists(remote): # Check if exists
                        print(f"The directory '{remote}' already exists remotely.")
                    else:
                        path = Path(remote)
                        path.mkdir(parents=True, exist_ok=True)
                        print(f"The directory '{remote}' has been created remotely.")
                except Exception as err:
                    if "input/output error" in str(err).lower():
                        ls_result = subprocess.run(["ls", remote], stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                                                   text=True, encoding='latin-1')
                        if ls_result.returncode != 0 and "stale file handle" in ls_result.stderr.lower():
                            raise RuntimeError(f"Stale filesystem mount detected, cannot copy data to {remote}")
                        else:
                            raise RuntimeError(f"Input/Output error detected: {err}")
                    raise RuntimeError(f"Error creating remote directory: {err}")
        
        # TODO: have movement service handle type without user input (cp,scp,rsync,conduit,pfcp,ftp,etc.)
        if tool.lower() == "copy":
            if all(x is None for x in file_list):
                file_list = [str(Path(self.local_location) / s) for s in filesystem_df["file_origin"]]
            for file, file_remote in zip(file_list, rfile_list):
                abspath = os.path.dirname(os.path.abspath(file_remote))
                if not os.path.exists(abspath):
                    if self.verbose:
                        print(" mkdir " + abspath)
                    path = Path(abspath)
                    try:
                        path.mkdir(parents=True)
                    except Exception:
                        raise RuntimeError(f"Unable to create folder {abspath}. Check your access rights")

                if self.verbose:
                    print(" cp " + file + " " + file_remote)
                shutil.copy2(file , file_remote)

            # delete temp columns from filesystem table
            filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table("filesystem", filesystem_df)
            self.t.dsi_tables.append("filesystem")

            # Database movement
            for dbname in db_list:
                if self.verbose:
                    print(" cp " + dbname + " " + os.path.join(self.remote_location, dbname))
                shutil.copy2(dbname, os.path.join(self.remote_location, dbname))

            print(" Data Copy Complete!")
        
        elif tool.lower() == "scp":
            try:
                host_part, path_part = self.remote_location.split(":", 1)
            except ValueError:
                raise ValueError("Remote path must be in the format user@host:/absolute/path")

            if not path_part.startswith("/") and "nt" not in os.name:
                raise ValueError("Remote path must be absolute (starting with /)")
            
            # making remote dir
            if self.verbose:
                print(" ssh "+ str(host_part) + " \"mkdir -p " + path_part + "\"" )
            cmd = ["ssh", host_part, f'mkdir -p \"{path_part}\"']
            print("Creating remote directory if it doesn't exist")
            self.execute_cmd(cmd, "Creating remote dir")

            # File movement
            cmd = ["scp", "-rp", self.local_location, self.remote_location]
            if self.verbose:
                print()
                print(*cmd)
            self.execute_cmd(cmd, "scp data")
            print(" DSI SCP data movement complete.")

            filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(host_part+":", "", regex=False)
            # delete temp columns from filesystem table
            filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
            # remove username@hostname from remote_location column in federated table
            federated_df = self.t.get_table("federated")
            federated_df["remote_location"] = federated_df["remote_location"].str.replace(host_part+":", "", regex=False)

            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table(["federated", "filesystem"], [federated_df, filesystem_df])
            self.t.dsi_tables.append("filesystem")

            # Database movement
            for dbname in db_list:
                cmd = ["scp", "-p", dbname, os.path.join(self.remote_location, dbname)]
                if self.verbose:
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
            
            # File movement
            self.local_location = self.local_location[:-1] if self.local_location.endswith("/") else self.local_location
            cmd = ["rsync", "-av", f"--rsync-path=mkdir -p {path_part} && rsync", self.local_location, self.remote_location]
            if self.verbose:
                print(*cmd)
            self.execute_cmd(cmd, "rsync data")
            print(" DSI Rsync data movement complete.")

            filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(host_part+":", "", regex=False)
            # delete temp columns from filesystem table
            filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
            # remove username@hostname from remote_location column in federated table
            federated_df = self.t.get_table("federated")
            federated_df["remote_location"] = federated_df["remote_location"].str.replace(host_part+":", "", regex=False)

            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table(["federated", "filesystem"], [federated_df, filesystem_df])
            self.t.dsi_tables.append("filesystem")
            
            # Database movement
            for dbname in db_list:
                cmd = ["rsync", "-av", dbname, self.remote_location]
                if self.verbose:
                    print()
                    print(*cmd)
                self.execute_cmd(cmd, "rsync database")
            print(" DSI Rsync database movement complete.")
        
        elif tool.lower() == "conduit":
            import signal

            # Test Kerberos
            if self.verbose:
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
                if self.verbose:
                    print("Testing Conduit: conduit get")
                cmd = ['/usr/projects/systems/conduit/bin/conduit-cli','--config','/usr/projects/systems/conduit/conf/conduit-cli-config.yaml','get']
                stdout = self.execute_cmd(cmd, "Testing conduit get")

                if "TRANSFER_ID" in stdout and self.verbose:
                    print(" Conduit is authenticated.")
                elif "TRANSFER_ID" not in stdout:
                    raise RuntimeError("Conduit Error: " + str(stdout))
            finally:
                signal.alarm(0)

            try:
                base_cmd = ['/usr/projects/systems/conduit/bin/conduit-cli','--config','/usr/projects/systems/conduit/conf/conduit-cli-config.yaml','cp','-r']
                # File Movement
                if self.verbose:
                    print("conduit cp -r " + self.local_location + " " + self.remote_location)
                cmd = base_cmd + [self.local_location, self.remote_location]
                stdout = self.execute_cmd(cmd, "Conduit copy data")
                if "finalized" in stdout.lower():
                    print(" DSI-Conduit data movement job complete.")
                else:
                    print(" WARNING: DSI-Conduit data movement job may not be complete")

                # delete temp columns from filesystem table
                filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
                self.t.dsi_tables.remove("filesystem")
                self.t.overwrite_table("filesystem", filesystem_df)
                self.t.dsi_tables.append("filesystem")

                # Database Movement
                for dbname in db_list:
                    if self.verbose:
                        print("conduit cp " + dbname + " " + os.path.join(self.remote_location, dbname))
                    cmd = base_cmd + [dbname, os.path.join(self.remote_location, dbname)]
                    stdout = self.execute_cmd(cmd, "Conduit copy database")
                    if "finalized" in stdout.lower():
                        print(" DSI-Conduit database movement job complete.")
                    else:
                        print(" WARNING: DSI-Conduit database movement job may not be complete")

                print("Type 'conduit get' to track status of both jobs.")
                print("  If 'WaitingForLease' status, data move is in queue.")
                print("  If 'Error' status, type 'conduit error <TRANSFER_ID>' to view detailed error output.")

            except Exception as e:
                raise RuntimeError(f"Conduit failed with error: {str(e)} ")

        elif tool.lower() == "pfcp":
            try:
                # File Movement
                if self.verbose:
                    print("pfcp -R " + self.local_location + " " + self.remote_location)
                cmd = ['pfcp','-R',self.local_location, self.remote_location]
                self.execute_cmd(cmd, "pfcp move data")
                print(" DSI submitted pfcp data movement job.")

                # delete temp columns from filesystem table
                filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
                self.t.dsi_tables.remove("filesystem")
                self.t.overwrite_table("filesystem", filesystem_df)
                self.t.dsi_tables.append("filesystem")

                # Database Movement
                for dbname in db_list:
                    if self.verbose:
                        print("pfcp " + dbname + " " + os.path.join(self.remote_location, dbname))
                    cmd = ['pfcp', dbname, os.path.join(self.remote_location, dbname)]
                    self.execute_cmd(cmd, "pfcp move database")
                    print(" DSI submitted pfcp database movement job.")
            except Exception as e:
                raise RuntimeError(f"pfcp failed with error: {str(e)} ")
        
        elif tool.lower() == "ftp":
            pass
            # delete temp columns from filesystem table -- do after data has been moved
            filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table("filesystem", filesystem_df)
            self.t.dsi_tables.append("filesystem")

        elif tool.lower() == "git":
            pass
            # delete temp columns from filesystem table -- do after data has been moved
            filesystem_df = filesystem_df.drop(columns=["file_abs"], errors="ignore")
            self.t.dsi_tables.remove("filesystem")
            self.t.overwrite_table("filesystem", filesystem_df)
            self.t.dsi_tables.append("filesystem")

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


    def get(self, config_file: str, workspace_folder: str):
        '''
        Helper function that searches remote location-based input config file, and retrieves metadata that contains DSI databases
        '''
        if not os.path.exists(config_file):
            raise ValueError(f"{config_file} does not exist. Please check the filepath and try again.")
        
        suffix = Path(config_file).suffix.lower()
        # Check if config file is YAML - contains paths to CSV files
        if suffix in [".yaml", ".yml"]:
            try:
                yaml_path = Path(config_file)
                config_data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                
                base_path = str(Path(yaml_path).parent)
            except yaml.YAMLError:
                raise ValueError(f"Invalid YAML file {yaml_path}. Please check the yaml file and try again.")
        elif suffix == ".csv":
            filename = str(Path(config_file).name)
            config_data = {  
                            'repo_paths': [filename], 
                            'workspace_folder': workspace_folder, 
                            'download_limit': 10485760, 
                            'conflict_resolution': 'keep_latest'}
            base_path = str(Path(config_file).parent)
        else:
            raise ValueError("Config file must be a YAML or CSV file")
        
        # Create a folder for the databases if it doesn't exist, or use the provided one
        if not workspace_folder:
            _workspace_folder = config_data.get("workspace_folder", "")
            workspace_folder = _workspace_folder or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"

        downloaded_dbs = federate_datasets(workspace_folder, config_data, base_path)

        if downloaded_dbs:
            df = pd.DataFrame(downloaded_dbs)
            df = df[["location_type", "location", "name", "workspace_folder", "folder_hash", "local_path", "submitter_name"]]
            df = df.rename(columns={"name": "db_name", "local_path": "local_db_path"})

            # Create 'federation' table in db
            curr_tables = self.t.list(True)
            if "federation" in curr_tables:
                # TODO: currently overwrites existing federation table
                federation_df = self.t.get_table("federation")
                federation_df = pd.concat([federation_df, df], ignore_index=True).drop_duplicates()
                self.t.overwrite_table("federation", federation_df)
            else:
                fnull = open(os.devnull, 'w')
                with redirect_stdout(fnull):
                    self.t.load_module('plugin', "Dataframe", "reader", collection=df, table_name="federation")
                    self.t.artifact_handler(interaction_type='ingest')


    def get_data(self, db_name: str, workspace_folder: str | None = None):
        curr_tables = self.t.list(True)
        if "federation" not in curr_tables or self.t.get_table("federation").empty:
            raise RuntimeError("Must first download DSI databases with the get() function")
        if " " in db_name:
            raise ValueError("Input db_name cannot have spaces")
        db_data = self.t.artifact_handler("query", query=f"SELECT * FROM federation WHERE db_name LIKE '%{db_name}%'")
        
        if db_data.empty:
            raise ValueError(f"Could not find a downloaded database named '{db_name}'")

        if len(db_data) > 1:
            try:
                print(f"\nMultiple local databases were found with the name `{db_name}`:")
                for idx, row in db_data.iterrows():
                    print(f"{idx+1}) {row['local_db_path']}")
                db_idx = input("\n -- Select which database's data to download (enter number): ")
                db_idx = int(db_idx)
                if not (1 <= db_idx <= len(db_data)):
                    print(" -- ERROR: Invalid selection. Skipping data download.")
                    return
            except (KeyboardInterrupt, EOFError):
                print("\n -- Interrupted while entering database selection. Skipping data download.\n")
                return
            except ValueError:
                print(f" -- ERROR: Input must be a number between 1 and {len(db_data)} (inclusive). Skipping data download.\n")
                return
            db_data = db_data.iloc[int(db_idx)-1]
        else:
            db_data = db_data.iloc[0]

        t2 = Terminal()
        backend_name = self.t.identify_backend(db_data["local_db_path"])
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            t2.load_module('backend', backend_name, 'back-write', filename=db_data["local_db_path"])

        db_tables = t2.list(True)
        if "filesystem" not in db_tables or "federated" not in db_tables:
            raise ValueError(f"Cannot download data from '{db_name}' because it has not been indexed by DSI.")
        remote_loc = t2.get_table("federated")["remote_location"].iloc[0]

        with open(f"{db_data["workspace_folder"]}/host_usernames.json", "r", encoding="utf-8") as f:
            host_username = yaml.safe_load(f)
        username = (host_username or {}).get(db_data['location'], "")

        if not workspace_folder:
            workspace_folder = f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"
        
        # add unique hash to download data
        workspace_folder = os.path.join(workspace_folder, db_data["folder_hash"])
        
        # Currently pulling all data referenced -- eventually allow user to download certain data
        db_info, username = pull_data(db_data["location_type"], db_data["location"], remote_loc, workspace_folder, username)
        new_folder = Path(db_info.pop("new_db_folder"))
        if new_folder.is_dir() and not any(new_folder.iterdir()):
            new_folder.rmdir()


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
            num_rows = len(next(iter(st_dict.values()))) # Assume all columns are of equal length
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
