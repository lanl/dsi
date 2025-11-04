# import dsi.backends.sqlalchemymysql as sqlalchemymysql
# from dsi.backends.sqlalchemymysql import Table, Column, Integer, String, Float, TEXT

import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, String, Float, TEXT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import TEXT, MEDIUMTEXT, LONGTEXT  # dialect-specific types (uppercase)
from sqlalchemy.types import Text  # generic Text type (lowercase class)

import secrets
import random
import socket
import sys
import yaml
import subprocess
import os
from datetime import datetime
import pandas as pd
import time
from collections import OrderedDict

from dsi.backends.filesystem import Filesystem

class DataType:
    """
        Primary DataType Artifact class that stores database schema in memory.
        A DataType is a generic construct that defines the schema for the tables inside of SQL. 
        Used to execute CREATE TABLE statements.
    """
    name = ""
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties


class ValueObject:
    """
    Data Structure used when returning search results from ``find``, ``find_table``, ``find_column``, ``find_cell``, or ``find_relation``

        - t_name: table name 
        - c_name: column name as a list. The length of the list varies based on the find function. 
          Read the description of each one to understand the differences
        - row_num: row number. Useful when finding a value in find_cell, find_relation, or find (includes results from find_cell)
        - type: type of match for this specific ValueObject. {table, column, range, cell, row, relation}
    """
    t_name = "" # table name
    c_name = [] # column name(s) 
    row_num = None # row number
    value = None # value stored from that match. Ex: table data, col data, cell data etc.
    type = "" #type of match, {table, column, range, cell, row}

    # implement this later once filesystem table incoroporated into dsi
    # filesystem_match = [] #list of all elements in that matching row in filesystem table


class SqlAlchemyMySQL(Filesystem):
    """
    SQLAlchemy Backend to access databases such as mysql, ...
    """

    runTable = False

    def __get_random_free_port(self, start: int = 1024, end: int = 65535) -> int:
        while True:
            port = random.randint(start, end)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('', port))
                    return port  # Found a free one
                except OSError:
                    continue  # Try another port


    def __infer_types(self, columns, rows):
        col_types = []
        for col_idx in range(len(columns)):
            types_in_col = {type(row[col_idx]).__name__ for row in rows}
            # If all are the same type, show one; else show multiple
            inferred_type = ', '.join(sorted(types_in_col))
            col_types.append(inferred_type)

        return col_types
    

    def __get_sqlalchemy_type(self, type_str):
        """
        Map inferred type labels (including composites like 'NoneType, str')
        to SQLAlchemy types. 'NoneType' only signals nullability and is ignored
        for the choice of the SQL type (caller should set nullable=True).
        """
        # Keep original for error messages
        original = type_str

        # Normalize to string
        if not isinstance(type_str, str):
            type_str = str(type_str)

        # Split composite labels like "NONETYPE, STR"
        parts = [p.strip().upper() for p in type_str.split(",") if p.strip()]
        parts_set = set(parts)

        # Treat NONETYPE as a nullability hint; ignore for type selection
        parts_set.discard("NONETYPE")

        # Canonicalize synonyms
        normalized = set()
        for p in parts_set:
            if p in {"INT", "INTEGER"}:
                normalized.add("INT")
            elif p in {"NONETYPE"}:
                normalized.add("LONGTEXT")
            elif p in {"STR", "STRING", "TEXT"}:
                normalized.add("LONGTEXT")
            elif p in {"FLOAT", "DOUBLE", "REAL", "DECIMAL"}:
                normalized.add("FLOAT")
            elif p in {"MEDIUMTEXT"}:
                normalized.add("MEDIUMTEXT")
            elif p in {"LONGTEXT"}:
                normalized.add("LONGTEXT")
            else:
                normalized.add(p)  # keep unknowns to trigger a clear error later

        # Decision rules (widest compatible type wins)
        # Explicit MySQL text types first
        if "LONGTEXT" in normalized:
            return LONGTEXT
        if "MEDIUMTEXT" in normalized:
            return MEDIUMTEXT

        # Any presence of string => String(255)
        if "STR" in normalized:
            return String(255)

        # Mixed numeric => Float
        if {"INT", "FLOAT"} <= normalized:
            return Float

        # Pure numeric
        if "FLOAT" in normalized:
            return Float
        if "INT" in normalized:
            return Integer

        # If everything was just NONETYPE (i.e., normalized is empty), default to String
        if not normalized:
            return String(255)

        # Unknown type label(s)
        raise ValueError(f"Unsupported type: {original}")


    def __create_table(self, table_name: str, headers: list[str], data_types: list[str]):
        metadata = sa.MetaData()

        # Define the table
        columns = []
        for name, col_type in zip(headers, data_types):
            columns.append(sa.Column(name, self.__get_sqlalchemy_type(col_type)))

        tbl = sa.Table(table_name, metadata, *columns)

        # Create it
        metadata.create_all(self.engine)


    def __insert_data_mysql(self, table_name: str, column_names: list[str], data_rows: list):
        metadata = sa.MetaData()
        try:
            # Reflect existing table
            table = sa.Table(table_name, metadata, autoload_with=self.engine)

            # Build list of dicts for bulk insert
            values_to_insert = [
                dict(zip(column_names, row)) for row in data_rows
            ]

            with self.engine.connect() as conn:
                stmt = sa.insert(table)
                result = conn.execute(stmt, values_to_insert)
                conn.commit()

            return result.rowcount
        
        except Exception as e:
            print(f"Error occurred: {e}")
            return 0 




    def __init__(self, filename: str):
        """
        Initialize a SQL Server and create a database

        `filename` : str
            The name of the database to create

        """
        # Open and safely load the YAML file and get path to the config
        try:
            with open("sqlalchemy_dsi_config.yaml", "r") as file:
                sqlalchemy_dsi_config = yaml.safe_load(file)
        except:
            print("Please make sre that sqlalchemy_dsi_config.yaml is in the same folder as the launch path. \n This YAML file is created by install_mysql.sh")
            print("DSI with the backend did not start!")
            sys.exit(0)
            return

        # initialize some parameters
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        self.path_to_db_installation = sqlalchemy_dsi_config["path_to_db_installation"]
        self.path_to_data = sqlalchemy_dsi_config["path_to_data"]
        self.sql_server_port = self.__get_random_free_port(1024, 65535)
        self.host = "localhost"
        self.database = filename
        self.filename = filename
        rand_num = secrets.randbelow(10000000)  # returns 0–9
        self.user = "dsi_user_" + str(rand_num)
        rand_num = secrets.randbelow(10000000)  # returns 0–9
        self.dsi_password = "dsi_password_" + str(rand_num)


        # Launch the script to start mysql server
        start_mqsql_path = os.path.join(self.current_dir, 'alchemy_utils', 'start_mysql.sh')
        args = [self.path_to_db_installation, self.path_to_data, str(self.sql_server_port)]
        print(f"launch command ([start_mqsql_path] + args): {[start_mqsql_path] + args}")
        subprocess.run([start_mqsql_path] + args, check=True)

        time.sleep(10)   # wait for things to happen


        # Launch the script to create DB and user
        create_db_user_path = os.path.join(self.current_dir, 'alchemy_utils', 'create_db_user.sh')
        args = [self.path_to_db_installation, self.database, self.user, self.dsi_password]
        print(f"launch command ([create_db_user_path] + args): {[create_db_user_path] + args}")
        subprocess.run([create_db_user_path] + args, check=True)

        time.sleep(5)   # wait for things to happen

        # Connect to the Server
        url=f"mysql+pymysql://{self.user}:{self.dsi_password}@{self.host}:{self.sql_server_port}/{self.database}"
        print(f"url: {url}")

        try:
            self.engine = sa.create_engine(url)

            try:
                self.conn = self.engine.connect()
                result = self.conn.execute(sa.text("SELECT NOW();"))
                print("Connection successful. Server time:", result.scalar())
            except Exception as e:
                print("Transaction Error:", e)

        except Exception as e:
            print("Engine connect failed:", e)


    def __del__(self):
        self.close()


    def query_artifacts(self, query: str, isVerbose: bool = False, dict_return: bool = False):
        """
        Execute an arbitrary SQL query against the current SQLAlchemy engine.

        Parameters
        ----------
        query : str
            The SQL query string to execute.
        isVerbose : bool, optional
            If True, prints the query output to stdout.
        dict_return : bool, optional
            If True, returns an OrderedDict for SELECT queries.
            Otherwise, returns a pandas DataFrame.

        Returns
        -------
        Union[pandas.DataFrame, OrderedDict, str, None]
            - DataFrame (or OrderedDict) for SELECT-type queries.
            - Success message string for non-SELECT queries.
            - None if an error occurs.
        """
        try:
            with self.engine.connect() as conn:
                output = conn.execute(sa.text(query))
                conn.commit()  # Important for DML (INSERT/UPDATE/DELETE)

                if output.returns_rows:
                    rows = output.fetchall()
                    columns = list(output.keys())

                    # Print if verbose
                    if isVerbose:
                        print(pd.DataFrame(rows, columns=columns))

                    # Return format based on dict_return flag
                    if dict_return:
                        result = OrderedDict()
                        for i, row in enumerate(rows):
                            result[i] = OrderedDict(zip(columns, row))
                        return result
                    else:
                        return pd.DataFrame(rows, columns=columns)

                else:
                    msg = f"Query executed successfully. {output.rowcount} rows affected."
                    if isVerbose:
                        print(msg)
                    return msg, None

        except Exception as e:
            print(f"Failed to run query '{query}', with error: {e}")
            return None


    def ingest_artifacts(self, collection, isVerbose: bool = False):
        list_of_tables = list(collection.keys())

        data_files = list(collection.values())
        for index, the_data in enumerate(data_files):
            columns = list(the_data.keys())

            lengths = [len(v) for v in the_data.values()]
            num_rows = max(lengths) if lengths else 0
            rows = [
                    [the_data[col][i] for col in columns]
                    for i in range(num_rows)
                ]

            data_types = self.__infer_types(columns, rows)
            self.__create_table(list_of_tables[index], columns, data_types)
            self.__insert_data_mysql(list_of_tables[index], columns, rows)
    



    def close(self):
        """ Close the database """
        try:
            # Launch the script to stop mysql server
            stop_mqsql_path = os.path.join(self.current_dir, 'alchemy_utils', 'stop_mysql.sh')
            args = [self.path_to_db_installation]
            subprocess.run([stop_mqsql_path] + args, check=True)
        except:
            pass

