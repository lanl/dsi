import json
import os
import pandas as pd
import sqlite3

from contextlib import closing
from pandasql import sqldf
from pathlib import Path

from dsi.dsi import DSI



def is_valid_sqlite_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid SQLite3 database file and contains at least one user table with data.

    Arg:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid SQLite3 database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid SQLite file with data" if valid.
    """
    if not os.path.isfile(path):
        return False, "file does not exist"

    try:
        with open(path, "rb") as f:
            header = f.read(16)
        if header != b"SQLite format 3\x00":
            return False, "not a SQLite3 file"
    except OSError as e:
        return False, f"could not read file: {e}"

    try:
        with closing(sqlite3.connect(path)) as conn:
            cur = conn.cursor()

            # Check integrity
            row = cur.execute("PRAGMA integrity_check;").fetchone()
            if not row or row[0].lower() != "ok":
                return False, "SQLite integrity check failed"

            # Find user tables
            tables = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                LIMIT 1
            """).fetchall()

            if not tables:
                return False, "valid SQLite file, but no user tables"

            # Check whether any user table has data
            for (table_name,) in cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
            """):
                qname = '"' + table_name.replace('"', '""') + '"'
                count = cur.execute(f"SELECT 1 FROM {qname} LIMIT 1").fetchone()
                if count is not None:
                    return True, "valid SQLite file with data"

            return False, "valid SQLite file, but tables are empty"

    except sqlite3.DatabaseError as e:
        return False, f"SQLite open/query failed: {e}"
    except Exception as e:
        return False, f"unexpected error: {e}"
    


def is_valid_duckdb_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid DuckDB database file and contains at least one user table with data.

    Arg:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid DuckDB database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid DuckDB file with data" if valid.
    """
    if not os.path.isfile(path):
        return False, "file does not exist"

    try:
        import duckdb
    except ImportError:
        return False, "duckdb package is not installed"

    # DuckDB does not have a simple fixed header check as convenient as SQLite.
    # The reliable test is: can DuckDB open it and query its catalog?
    try:
        conn = duckdb.connect(path, read_only=True)
        try:
            # Check that catalog is readable by listing tables
            tables = conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type = 'BASE TABLE'
            """).fetchall()

            if not tables:
                return False, "valid DuckDB file, but no user tables"

            # Check whether any table has at least one row
            for schema_name, table_name in tables:
                qschema = '"' + schema_name.replace('"', '""') + '"'
                qtable = '"' + table_name.replace('"', '""') + '"'
                row = conn.execute(
                    f"SELECT 1 FROM {qschema}.{qtable} LIMIT 1"
                ).fetchone()
                if row is not None:
                    return True, "valid DuckDB file with data"

            return False, "valid DuckDB file, but tables are empty"
        finally:
            conn.close()

    except Exception as e:
        return False, f"DuckDB open/query failed: {e}"
    


def detect_valid_db_with_data(path: str) -> tuple[str | None, bool, str]:
    """
    Detects whether the file at `path` is a valid SQLite or DuckDB database file containing at least one user table with data.

    Args:
        path: The file path to check.
        
    Returns:
        A tuple (db_type, is_valid, message) where:
    """
    ok, msg = is_valid_sqlite_with_data(path)
    if ok:
        return "sqlite", True

    ok2, msg2 = is_valid_duckdb_with_data(path)
    if ok2:
        return "duckdb", True

    return None, False



class f_dsi:
    """A class for federated querying of DSI databases. It loads metadata about the databases and 
    their tables from a specified folder, and provides methods to summarize, query, search, and find data across the federated databases."""

    def __init__(self, federated_folder_path:str):
        """Initializes the f_dsi class by loading metadata about the federated databases and their tables from a specified folder.
        
        Args:
            federated_folder_path (str): The file path to the folder containing the metadata about the federated databases. The folder should contain a JSON file named "dsi_database_list.json" with the metadata information.
        """

        self.federated_folder_path = federated_folder_path
        
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
                db_info['path'] = str(db_path)
                db_info['name'] = dsi_db_info['name']
                _tbls = _temp.list(True)
                db_info['num_tables'] = len(_tbls)
                db_info['tables'] = _tbls
                _temp.close()
                
                databases.append(db_info)
            else:
                print(f"!!!!Error opening database at {db_path}!!!!")
        
        self.df = pd.DataFrame(databases)   # what is exposed to the user
        self.df_exp = self.df.explode("tables").rename(columns={"tables": "table"})  # what is used internally
        


    def __find_db_path(self, table: str, db: str) -> str:
        """Finds the file path of a database containing a specified table and database name within the federated system.
        
        Args:
            table (str): The name of the table to find.
            db (str): The name of the database containing the table.

        Returns:
            The file path of the database containing the specified table and database name.
        """
        q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_str = out.loc[0, "path"]
        return path_str
    
        

    def get_db_path(self, db_name: str) -> list[str]:
        """Returns a list of file paths for databases with the specified name within the federated system.
        
        Args:
            db_name (str): The name of the database to find.

        Returns:            
            A list of file paths for databases with the specified name.
        """

        q = f"SELECT path FROM df_exp WHERE name = '{db_name}'"        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_list = out["path"].tolist()
        return path_list
    


    def f_get_databases(self):
        """Returns a DataFrame containing information about the federated databases, including their paths, names, and tables."""
        return self.df
    


    def f_summarize(self, table: str, db: str):
        """Summarizes the contents of a specified table in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to summarize.
            db (str): The name of the database containing the table.
        
        Returns:
            The summary of the specified table.
        """

        # Find the path of the database to query
        db_path_str = self.__find_db_path(table, db)
    
        _temp = DSI(db_path_str, silence_messages="True")
        result = _temp.summary(collection=True)
        return result



    def f_query(self, table: str, db: str, query: str):
        """Executes a query on a specified table in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to query.
            db (str): The name of the database containing the table.
            query (str): The query to execute on the specified table.   

        Returns:
            The result of the query execution.
        """
        
        # Find the path of the database to query
        db_path_str = self.__find_db_path(table, db)
    
        _temp = DSI(db_path_str, silence_messages="True")
        result = _temp.query(query, collection=True)
        return result



    def f_search(self, table: str, db: str, query: str):
        """Searches for a specified query in a specified table in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to search.
            db (str): The name of the database containing the table.
            query (str): The query to search for in the specified table.    
        """

        # Find the path of the database to query
        db_path_str = self.__find_db_path(table, db)
    
        # Use DSI to run the query on the specified database and table
        _temp = DSI(db_path_str, silence_messages="True")
        result = _temp.search(query, collection=True)
        return result



    def f_find(self, table: str, db: str, query: str):
        """Finds a specified query in a specified table in a specified database within the federated system.
        
        Args:
            table (str): The name of the table to find.
            db (str): The name of the database containing the table.
            query (str): The query to find in the specified table.

        Returns:
            The result of the find operation.
        """

        # Find the path of the database to query
        db_path_str = self.__find_db_path(table, db)
    

        # Use DSI to run the find operation on the specified database and table
        _temp = DSI(db_path_str, silence_messages="True")
        result = _temp.find(query, collection=True)
        return result