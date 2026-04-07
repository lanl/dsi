import json
import pandas as pd

#from pandasql import sqldf
from pathlib import Path

from dsi.dsi import DSI
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
            _federated_folder_path = Path(federated_folder_path)
            with open( f"{_federated_folder_path}/dsi_database_list.json", "r", encoding="utf-8") as f:
                dsi_databases_list = json.load(f)
                
        except Exception as e:
            print(f"Error {e}, could not read the database at {_federated_folder_path}/dsi_database_list.json")
            return
        

        databases = []
        # print(db_path_list)
        for d_id, dsi_db_info in enumerate(dsi_databases_list):
            db_info = {}

            db_path = Path(dsi_db_info['local_path'])
            database_type, valid_db = detect_valid_db_with_data(db_path)

            if valid_db:
                _temp = DSI(str(db_path), backend_name=database_type, silence_messages="True")
                db_info['id'] = d_id
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
        

    def _list_databases(self):
        """Returns a DataFrame containing information about the federated databases, including their paths, names, and tables."""

        return self.df


    # def _find_db_path(self,  db: str, table: str="") -> list[str]:
    #     """Finds the file path of a database containing a specified table and database name within the federated system.
        
    #     Args:
    #         table (str): The name of the table to find.
    #         db (str): The name of the database containing the table.

    #     Returns:
    #         The file path of the database containing the specified table and database name.
    #     """
    #     if table == "":
    #         q = f"SELECT path FROM df_exp WHERE name = '{db}'"
    #     else:
    #         q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"

    #     out = sqldf(q, {"df_exp": self.df_exp})
 
    #     path_str = out["path"].tolist()
    #     return path_str
    
    def _find_db_path(self, db: str, table: str = "") -> list[str]:
        """Finds the file path of a database containing a specified table and database name within the federated system.

        Args:
            table (str): The name of the table to find.
            db (str): The name of the database containing the table.

        Returns:
            list[str]: The file path(s) of the database containing the specified table and database name.
        """
        df = self.df_exp

        if table == "":
            filtered = df[df["name"] == db]
        else:
            filtered = df[(df["table"] == table) & (df["name"] == db)]

        return filtered["path"].tolist()

    # def get_db_path(self, db_name: str) -> list[str]:
    #     """Returns a list of file paths for databases with the specified name within the federated system.
        
    #     Args:
    #         db_name (str): The name of the database to find.

    #     Returns:            
    #         A list of file paths for databases with the specified name.
    #     """

    #     q = f"SELECT path FROM df_exp WHERE name = '{db_name}'"        
    #     out = sqldf(q, {"df_exp": self.df_exp})
    #     path_list = out["path"].tolist()
    #     return path_list

    def get_db_path(self, db_name: str) -> list[str]:
        """Returns a list of file paths for databases with the specified name within the federated system.
    
        Args:
            db_name (str): The name of the database to find.

        Returns:            
            list[str]: A list of file paths for databases with the specified name.
        """
        df = self.df_exp

        if "name" not in df.columns or "path" not in df.columns:
            raise KeyError("DataFrame must contain 'name' and 'path' columns")

        return df.loc[df["name"] == db_name, "path"].tolist()


    def f_get_databases(self) -> list[dict]:
        """Returns a list of dictionaries containing information about the federated databases, including their paths, names, and tables.
        
        Returns:
            list[dict]: A list of dictionaries, where each dictionary contains information about a federated database, including its path, name, original location, and tables.
        """
        return (self.df).to_list()
    

    def f_display_databases(self) -> None:
        """Prints a DataFrame containing information about the federated databases, including their paths, names, and tables."""
        if self.operating_mode == "notebook":
            try:
                from IPython.display import display
                display(self.df)
            except ImportError:
                print(self.df)  
        else:
            print(self.df)


    def f_search_for_databases(self, name: str | None = None, original_location: str | None = None, return_output=False) -> dict | None:
        """Searches for databases in the federated system based on specified criteria such as name and original location.
        
        Args:
            name (str | None): A string to search for in the database names. If None, this criterion is ignored. Default is None.
            original_location (str | None): A string to search for in the original location of the databases. If None, this criterion is ignored. Default is None.
            return_output (bool): Whether to return the output as a dictionary. Default is False.
        """
        df = self.df

        if name is not None:
            df = df[df["name"].str.contains(name, case=False, na=False)]

        if original_location is not None:
            df = df[df["original_location"].str.contains(original_location, case=False, na=False)]


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


    def f_summary(self, db: str, table: str=""):
        """Summarizes the contents in a specified database and a database if provided within the federated system.
        
        Args:
            table (str): The name of the table to summarize.
            db (str): The name of the database containing the table.
        
        Returns:
            The summary of the specified table.
        """

        # Find the path of the database to query
        db_path_str = self._find_db_path(db, table)
    
        res = []
        for db in db_path_str:
            _temp = DSI(db, silence_messages="True")

            if table == "":
                result = _temp.summary(collection=True)
            else:
                result = _temp.summary(table_name=table, collection=True)
            _temp.close()
            res.append(result)
        
        return res



    def f_query(self, query: str, db: str, table: str=""):
        """Executes a query on a specified table in a specified database within the federated system.
        
        Args:
            query (str): The query to execute on the specified table.
            db (str): The name of the database containing the table.
            table (str): The name of the table to query.

        Returns:
            The result of the query execution.
        """
        
        # Find the path of the database to query
        db_path_str = self._find_db_path(db, table)
    
        res = []
        for db in db_path_str:
            _temp = DSI(db, silence_messages="True")
            result = _temp.query(query, collection=True)
            res.append(result)
            _temp.close()

        return res



    def f_search(self, query: str, db: str, table: str=""):
        """Searches for a specified query in a specified table in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to search.
            db (str): The name of the database containing the table.
            query (str): The query to search for in the specified table.    
        """

        # Find the path of the database to query
        db_path_str = self._find_db_path(db, table)
    
        # Use DSI to run the query on the specified database and table
        res = []
        for db in db_path_str:
            print(f"Running search on database at {db}...")
            _temp = DSI(db, silence_messages="True")
            result = _temp.search(query, collection=True)
            res.append(result)
            _temp.close()

        return res
        



    def f_find(self, query: str, db: str, table: str=""):
        """Finds a specified query in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to find.
            db (str): The name of the database containing the table.
            query (str): The query to find in the specified table.

        Returns:
            The result of the find operation.
        """

        # Find the path of the database to query
        db_path_str = self._find_db_path(db, table)
    

        # Use DSI to run the find operation on the specified database and table
        res = []
        for db in db_path_str:
            _temp = DSI(db, silence_messages="True")
            result = _temp.find(query, collection=True)
            res.append(result)
            _temp.close()

        return res