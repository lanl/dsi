import json
import os
import random
import yaml
import hashlib
from coolname import generate_slug
import pandas as pd

#from pandasql import sqldf
from pathlib import Path

from dsi.dsi import DSI
from dsi.sync import Sync
from dsi.utils.dsi_utils import detect_valid_db_with_data

class DSIFederated:
    """A class for federated querying of DSI databases. It loads metadata about the databases and 
    their tables from a specified folder, and provides methods to summarize, query, search, and find data across the federated databases."""

    def __init__(self, federated_folder_path:str, operating_mode:str="console"):
        """Initializes the DSIFederated class by loading metadata about the federated databases and their tables from a specified folder.
        
        Args:
            federated_folder_path (str): The file path to the folder containing the metadata about the federated databases. The folder should contain a JSON file named "dsi_database_list.json" with the metadata information.
            operating_mode (str): console or notebook, determines how the results are displayed. Default is "console".
        """

        self.federated_folder_path = federated_folder_path
        self.operating_mode = operating_mode

        try:
            _federated_folder_path = Path(self.federated_folder_path)
            with open( f"{_federated_folder_path}/dsi_database_list.json", "r", encoding="utf-8") as f:
                dsi_databases_list = json.load(f)

            self.init_federated(dsi_databases_list)
                
        except Exception as e:
            print(f"Error {e}, could not read the database at {self.federated_folder_path}/dsi_database_list.json")
            return

    
    def init_federated(self, dsi_databases_list: list[dict]):
        """Initializes the federated system by loading metadata about the federated databases and their tables from a specified folder.
          This method is called during the initialization of the DSIFederated class.
          
        Args:
            dsi_databases_list (list[dict]): A list of dictionaries containing information about each federated database.
        """
        
        databases = []
        # print(db_path_list)
        for d_id, dsi_db_info in enumerate(dsi_databases_list):
            db_info = {}

            db_path = Path(dsi_db_info['local_path'])
            database_type, valid_db = detect_valid_db_with_data(db_path)

            if valid_db:
                _temp = DSI(str(db_path), backend_name=database_type, silence_messages="True")

                # make an easy name for 
                seed = int(hashlib.md5(str(db_path).encode()).hexdigest()[:8], 16)
                random.seed(seed)

                db_info['id'] = generate_slug(2)

                db_info['original_location'] = dsi_db_info['original_location_type']
                db_info['original_path'] = dsi_db_info['original_path']
                db_info['name'] = dsi_db_info['name']
                db_info['path'] = str(db_path)
                
                _tbls = _temp.list(True)
                db_info['num_tables'] = len(_tbls)
                db_info['tables'] = _tbls
                _temp.close()
                
                databases.append(db_info)
            else:
                print(f"!!!!Error opening database at {db_path}!!!!")
        
        self.df = pd.DataFrame(databases)   # what is exposed to the user
        self.df_exp = self.df.explode("tables").rename(columns={"tables": "table"})  # what is used internally
        

    def _list_tables(self):
        """Returns a DataFrame containing information about the federated databases, including their paths, names, and tables by table."""
        return self.df_exp


    def f_federate(self, 
                   config_file: str, 
                   orkspace_folder: str = ""):
        """Federates databases based on a specified configuration file containing the criteria for federating the databases.
        
        Args:
            config_path (str): The file path to the configuration file containing the criteria for federating the databases. The configuration file should be in JSON format and contain the necessary information for federating the databases.
        """
        # Check if the file exists and is a valid yaml file before trying to federate
        if not os.path.exists(config_file):
            print(f"federate ERROR: {config_file} does not exist. Please check the filepath and try again.")
            return
        else:
            try:
                with open(config_file, 'r') as f:
                    data = f.read()
                config_data = yaml.safe_load(data)
            except yaml.YAMLError as e:
                print(f"Invalid YAML file {config_file}. Please check the yaml file and try again.")
                return
            
        try:
            s = Sync()
            if workspace_folder == "":
                _workspace_folder = config_data.get("workspace_folder", "")
                if _workspace_folder == "":
                    workspace_folder = "dsi_data"
                    print(f"Synchronization data from {config_file} into {workspace_folder}")
                    s.get(config_file, workspace_folder)
                else:
                    print(f"Synchronization data from {config_file} into {workspace_folder}")
                    workspace_folder = _workspace_folder
                    s.get(config_file, workspace_folder)
            else:
                print(f"Synchronization data from {config_file} into {workspace_folder}")
                s.get(config_file, workspace_folder)
        except Exception as e:
            print(f"federate ERROR: {e}")
            return
        

        try:
            _federated_folder_path = Path(self.federated_folder_path)
            with open( f"{_federated_folder_path}/dsi_database_list.json", "r", encoding="utf-8") as f:
                dsi_databases_list = json.load(f)

            self.init_federated(dsi_databases_list)
                
        except Exception as e:
            print(f"Error {e}, could not read the database at {self.federated_folder_path}/dsi_database_list.json")
            return
    

    def f_list_databases(self, 
                         return_output: bool=False) -> list[dict] | None:
        """Prints a DataFrame containing information about the federated databases, including their paths, names, and tables.
        
        Args:
            return_output (bool): Whether to return the output as a list of dictionaries. Default is False.
        
        Returns:
            list[dict]: A list of dictionaries, where each dictionary contains information about a federated database, including its path, name, original location, and tables.
        """
        if self.operating_mode == "notebook":
            try:
                from IPython.display import display
                display(self.df)
            except ImportError:
                print(self.df)  
        else:
            print(self.df)
            
        if return_output:
            return self.df.to_dict(orient="records")


    def f_search_for_databases(self, 
                               db: str | None = None, 
                               table: str | None = None, 
                               original_location: str | None = None, 
                               return_output:bool = False,
                               display_results:bool = True) -> dict | None:
        """Searches for databases in the federated system based on specified criteria such as name and original location.
        
        Args:
            db (str | None): A string to search for in the database names. If None, this criterion is ignored. Default is None.
            table (str | None): A string to search for in the table names. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.
            return_output (bool): Whether to return the output as a dictionary. Default is False.
            display_results (bool): Whether to display the search results. Default is False.
        """
        df = self.df

        if db is not None:
            df = df[df["name"].str.contains(db, case=False, na=False)]

        if table is not None:
            df = df[df["tables"].apply(
                lambda x: isinstance(x, (list, tuple, set)) and any(table in t for t in x))]
            
        if original_location is not None:
            df = df[df["original_location"].str.contains(original_location, case=False, na=False)]

        if display_results:
            if self.operating_mode == "notebook":
                try:
                    from IPython.display import display
                    display(df)
                except ImportError:
                    print(df)  
            else:
                print(df)

        if return_output:
            return df.to_dict(orient="records")


    def f_get_db_path(self, 
                      db: str | None = None, 
                      table: str | None = None,
                      original_location: str | None = None) -> list[str]:
        """Finds the file path of a database containing a specified table and database name within the federated system.

        Args:
            table (str): The name of the table to find.
            db (str): The name of the database containing the table.

        Returns:
            list[str]: The file path(s) of the database containing the specified table and database name.
        """
        
        found_dbs = self.f_search_for_databases(db, table, original_location, return_output=True, display_results=False)

        return [row["path"] for row in found_dbs]


    def f_summary(self, 
                  db: str | None = None, 
                  table: str | None = None,
                  original_location: str | None = None) -> None:
        """Display the summary of the contents in a specified database and a database if provided within the federated system and displays it.
        
        Args:
            db (str | None): The name of the database containing the table. If None, this criterion is ignored. Default is None.
            table (str | None): The name of the table to summarize. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.
            
        Returns:
            The summary of the specified table.
        """

        # Find the path of the database to query
        if db is None and table is None:
            found_dbs = self.df.to_dict(orient="records")
        else:
            found_dbs = self.f_search_for_databases(db, table, original_location, return_output=True, display_results=False)
    
        
        for db_info in found_dbs:
            print(f"\nDatabase: {db_info['name']} at path {db_info['path']}:")
            _temp = DSI(db_info['path'], silence_messages="True")

            if table == "":
                result = _temp.summary(collection=True)
            else:
                result = _temp.summary(table_name=table, collection=True)
            _temp.close()
            
            if self.operating_mode == "notebook":
                try:
                    from IPython.display import display
                    display(result)
                except ImportError:
                    print(result)  
            else:
                print(result)
            
        
    def f_query(self, 
                query: str, 
                db: str | None = None, 
                table: str | None = None,
                original_location: str | None = None):
        """DSI Query within the federated system. 
            If no table and database are specified, queries all tables in the database.
        
        Args:
            query (str): The query to execute on the specified table.
            db (str | None): The name of the database containing the table. If None, this criterion is ignored. Default is None.
            table (str | None): The name of the table to query. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.  

        Returns:
            The result of the query execution.
        """
        
        # Find the path of the database to query
        # Find the databases
        if db is None and table is None:
            found_dbs = self.df.to_dict(orient="records")
        else:
            found_dbs = self.f_search_for_databases(db, table, original_location, return_output=True, display_results=False)
    
        res = []
        for db_info in found_dbs:
            _temp = DSI(db_info['path'], silence_messages="True")
            result = _temp.query(query, collection=True)
            res.append(result)
            _temp.close()

        return res


    def f_search(self, 
                 query: str, 
                 db: str | None = None, 
                 table: str | None = None, 
                 original_location: str | None = None):
        """
        Calls DSI Search within a specified database within the federated system.
        If no table and database are specified, queries all tables in the database.
        
        Args:
            query (str): The query to search for in the specified table.
            db (str | None): The name of the database containing the table. If None, this criterion is ignored. Default is None.
            table (str | None): The name of the table to search. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.
        """

        # Find the databases
        if db is None and table is None:
            found_dbs = self.df.to_dict(orient="records")
        else:
            found_dbs = self.f_search_for_databases(db, table, original_location, return_output=True, display_results=False)
    
        # Use DSI to run the query on the specified database and table
        res = []
        for db_info in found_dbs:
            _temp = DSI(db_info['path'], silence_messages="True")
            result = _temp.search(query, collection=True)
            res.append(result)
            _temp.close()

        return res
        

    def f_find(self, 
               query: str, 
               db: str | None = None, 
               table: str | None = None,
               original_location: str | None = None):
        """Calls DSI Find in a database specified by db within the federated system.
            If no table and database are specified, queries all tables in the database.
        
        Args:
            query (str): The query to find in the specified table.
            db (str | None): The name of the database containing the table. If None, this criterion is ignored. Default is None.
            table (str | None): The name of the table to find. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.

        Returns:
            The result of the find operation.
        """

        # Find the databases
        if db is None and table is None:
            found_dbs = self.df.to_dict(orient="records")
        else:
            found_dbs = self.f_search_for_databases(db, table, original_location, return_output=True, display_results=False)
    

        # Use DSI to run the find operation on the specified database and table
        res = []
        for db_info in found_dbs:
            _temp = DSI(db_info['path'], silence_messages="True")
            result = _temp.find(query, collection=True)
            res.append(result)
            _temp.close()

        return res
    

    def f_merge(self, src_db_id: str, src_tbl_name: str,
            dst_db_id: str, dst_tbl_name: str,
            mode: str = "inner"):
        """
        Merge src table into dst table and overwrite the destination table.

        Args:
            src_db_id (str): The ID of the source database containing the table to be merged.
            src_tbl_name (str): The name of the source table to be merged.
            dst_db_id (str): The ID of the destination database containing the table to merge into.
            dst_tbl_name (str): The name of the destination table to merge into.
            mode (str): How to handle column mismatches:
                - "inner": keep only shared columns
                - "outer": keep union of columns
                - "exact": require identical columns
        """

        mode = mode.lower()
        if mode not in {"inner", "outer", "exact"}:
            raise ValueError("mode must be one of: 'inner', 'outer', 'exact'")

        matches_src = self.df_exp[
            (self.df_exp["id"] == src_db_id) &
            (self.df_exp["table"] == src_tbl_name)
        ]

        matches_dst = self.df_exp[
            (self.df_exp["id"] == dst_db_id) &
            (self.df_exp["table"] == dst_tbl_name)
        ]

        if matches_src.empty:
            raise ValueError(
                f"No source database/table match found for id='{src_db_id}', table='{src_tbl_name}'"
            )
        if matches_dst.empty:
            raise ValueError(
                f"No destination database/table match found for id='{dst_db_id}', table='{dst_tbl_name}'"
            )

        src_data = {
            "path": matches_src.iloc[0]["path"],
            "name": matches_src.iloc[0]["name"],
            "table": matches_src.iloc[0]["table"],
        }

        dst_data = {
            "path": matches_dst.iloc[0]["path"],
            "name": matches_dst.iloc[0]["name"],
            "table": matches_dst.iloc[0]["table"],
        }

        print(f"source path: {src_data['path']}, table: {src_data['table']}")
        print(f"destination path: {dst_data['path']}, table: {dst_data['table']}")

        _temp_src = DSI(src_data["path"], silence_messages=True)
        try:
            tbl_src = _temp_src.get_table(src_data["table"], collection=True)
        finally:
            _temp_src.close()

        _temp_dst = DSI(dst_data["path"], silence_messages=True)
        try:
            tbl_dst = _temp_dst.get_table(dst_data["table"], collection=True)
        finally:
            _temp_dst.close()

        if tbl_src is None or tbl_src.empty:
            raise ValueError(
                f"Source table '{src_tbl_name}' in database '{src_db_id}' is empty or missing"
            )
        if tbl_dst is None or tbl_dst.empty:
            raise ValueError(
                f"Destination table '{dst_tbl_name}' in database '{dst_db_id}' is empty or missing"
            )

        src_cols = list(tbl_src.columns)
        dst_cols = list(tbl_dst.columns)
        src_set = set(src_cols)
        dst_set = set(dst_cols)

        if mode == "exact":
            if src_set != dst_set:
                raise ValueError(
                    f"Schemas do not match exactly.\n"
                    f"source columns: {src_cols}\n"
                    f"destination columns: {dst_cols}"
                )
            tbl_src = tbl_src[dst_cols]
            tbl_dst = tbl_dst[dst_cols]

        elif mode == "inner":
            common_cols = [col for col in dst_cols if col in src_set]
            if not common_cols:
                raise ValueError("No common columns found between source and destination tables")
            tbl_src = tbl_src[common_cols]
            tbl_dst = tbl_dst[common_cols]

        elif mode == "outer":
            all_cols = list(dict.fromkeys(dst_cols + src_cols))
            tbl_src = tbl_src.reindex(columns=all_cols)
            tbl_dst = tbl_dst.reindex(columns=all_cols)

        df_out = pd.concat([tbl_dst, tbl_src], ignore_index=True).drop_duplicates(ignore_index=True)

        # DSI.update() full-table overwrite path requires dsi_table_name
        df_to_update = df_out.copy()
        df_to_update.insert(0, "dsi_table_name", dst_data["table"])

        _temp_dst = DSI(dst_data["path"], silence_messages=True)
        try:
            _temp_dst.update(df_to_update)
        finally:
            _temp_dst.close()

        print(
            f"Successfully merged table '{src_data['table']}' from database '{src_data['name']}' "
            f"into table '{dst_data['table']}' in database '{dst_data['name']}' using mode '{mode}'."
        )
        
        
    def f_add_table(self, 
                    src_db_id: str, 
                    src_tbl_name: str,
                    dst_db_id: str, 
                    dst_tbl_name: str | None = None,
                    overwrite: bool = False) -> None:
        """
        Copy a table from one database into another database.

        Args:
            src_db_id (str): Source database ID.
            src_tbl_name (str): Source table name.
            dst_db_id (str): Destination database ID.
            dst_tbl_name (str | None): Destination table name. If None, use src_tbl_name.
            overwrite (bool): If True, overwrite destination table if it exists.
        """

        if dst_tbl_name is None:
            dst_tbl_name = src_tbl_name

        matches_src = self.df_exp[
            (self.df_exp["id"] == src_db_id) &
            (self.df_exp["table"] == src_tbl_name)
        ]

        matches_dst = self.df_exp[
            self.df_exp["id"] == dst_db_id
        ]

        if matches_src.empty:
            raise ValueError(
                f"No source database/table match found for id='{src_db_id}', table='{src_tbl_name}'"
            )

        if matches_dst.empty:
            raise ValueError(
                f"No destination database match found for id='{dst_db_id}'"
            )

        src_data = {
            "path": matches_src.iloc[0]["path"],
            "name": matches_src.iloc[0]["name"],
            "table": matches_src.iloc[0]["table"],
        }

        dst_data = {
            "path": matches_dst.iloc[0]["path"],
            "name": matches_dst.iloc[0]["name"],
        }

        # Check whether destination table already exists
        existing_dst = self.df_exp[
            (self.df_exp["id"] == dst_db_id) &
            (self.df_exp["table"] == dst_tbl_name)
        ]

        if not existing_dst.empty and not overwrite:
            raise ValueError(
                f"Destination table '{dst_tbl_name}' already exists in database '{dst_db_id}'. "
                f"Use overwrite=True to replace it."
            )

        # Read source table
        _temp_src = DSI(src_data["path"], silence_messages=True)
        try:
            tbl_src = _temp_src.get_table(src_data["table"], collection=True)
        finally:
            _temp_src.close()

        if tbl_src is None or tbl_src.empty:
            raise ValueError(
                f"Source table '{src_tbl_name}' in database '{src_db_id}' is empty or missing"
            )

        # Write into destination database
        _temp_dst = DSI(dst_data["path"], silence_messages=True)
        try:
            if existing_dst.empty:
                # New table: use read(..., "Collection")
                _temp_dst.read(tbl_src, "Collection", table_name=dst_tbl_name)
            else:
                # Existing table: overwrite with update()
                df_to_update = tbl_src.copy()
                df_to_update.insert(0, "dsi_table_name", dst_tbl_name)
                _temp_dst.update(df_to_update, backup=True)
        finally:
            _temp_dst.close()

        print(
            f"Successfully added table '{src_tbl_name}' from database '{src_data['name']}' "
            f"to database '{dst_data['name']}' as table '{dst_tbl_name}'."
        )