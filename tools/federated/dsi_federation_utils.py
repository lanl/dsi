
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
    def __init__(self, folder_name:str):
        self.folder_name = folder_name
        folder = Path(folder_name)
        print("folder:", folder)

        print("\n")
        db_path_list = []
        for p in folder.rglob("*"):
            if p.is_file():
                db_path_list.append(p)
                print(p)

        print("\n")
        databases = []
        print(db_path_list)
        for d_id, db_path in enumerate(db_path_list):
            db_info = {}

            print("db_path:",db_path)
            database_name, valid_db = detect_valid_db_with_data(db_path)

            if valid_db:
                _temp = DSI(str(db_path), backend_name=database_name, silence_messages="True")
                db_info['id'] = d_id
                db_info['path'] = str(db_path)
                db_info['name'] = (str(db_path)).split('/')[-1]
                _tbls = _temp.list(True)
                db_info['num_tables'] = len(_tbls)
                db_info['tables'] = _tbls
                _temp.close()
                
                databases.append(db_info)
            else:
                print(f"!!!!Error opening database at {db_path}!!!!")

                # delete the file
                # file_path = Path(db_path)
                # file_path.unlink()

        
        print("\nDatabases:")
        print(databases)
        
        self.df = pd.DataFrame(databases)
        self.df_exp = self.df.explode("tables").rename(columns={"tables": "table"})
        
        
    def f_get_databases(self):
        """Returns a DataFrame with one row per table in each database, containing columns: id, path, name, num_tables, table."""
        return self.df
    

    def f_summarize(self, table, db):
        """Returns a summary of the specified table in the specified database.
        
        Args:
            table: The name of the table to summarize.
            db: The name of the database containing the table.
        """

        q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"
        print(q)
        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_str = out.loc[0, "path"]
        print(path_str)
    
        _temp = DSI(path_str, silence_messages="True")
        l = _temp.summary(collection=True)
        return l


    def f_query(self, table, db, query):
        """Returns the result of executing the specified SQL query on the specified table in the specified database.

        """
        q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"
        print(q)
        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_str = out.loc[0, "path"]
    
        _temp = DSI(path_str, silence_messages="True")
        l = _temp.query(query, collection=True)
        return l


    def f_search(self, table, db, query):
        q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"
        print(q)
        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_str = out.loc[0, "path"]
    
        _temp = DSI(path_str, silence_messages="True")
        l = _temp.search(query, collection=True)
        return l


    def f_find(self, table, db, query):
        """Returns the list of rows in the specified table and database that match the specified query, where the query is a natural language description of the desired data.
        
        Args:
            table: The name of the table to search.
            db: The name of the database containing the table.
            query: A natural language description of the desired data, e.g. "all rows where age > 30".

        
        """
        q = f"SELECT path FROM df_exp WHERE \"table\" = '{table}' AND name = '{db}'"
        print(q)
        
        out = sqldf(q, {"df_exp": self.df_exp})
        path_str = out.loc[0, "path"]
    
        _temp = DSI(path_str, silence_messages="True")
        l = _temp.find(query, collection=True)
        return l