# import dsi.backends.sqlalchemymysql as sqlalchemymysql
# from dsi.backends.sqlalchemymysql import Table, Column, Integer, String, Float, TEXT

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import TEXT, MEDIUMTEXT, LONGTEXT  # dialect-specific types (uppercase)
from sqlalchemy.types import Text  # generic Text type (lowercase class)

import secrets
import random
import socket
import re
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


    def __create_table(self, table_name: str, headers: list[str], data_types: list[str]):
        metadata = sa.MetaData()

        # Define the table
        columns = []
        for name, col_type in zip(headers, data_types):
            columns.append(sa.Column(name, self.get_sqlalchemy_type(col_type)))

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

    

    def query_artifacts(self, sql_query: str, isVerbose: bool = False, dict_return: bool = False):
        try:
            with self.engine.connect() as conn:
                output = conn.execute(sa.text(sql_query))
                conn.commit()  # important for DELETE/INSERT/UPDATE

                if output.returns_rows:
                    return output.fetchall(), list(output.keys())
                else:
                    return f"Query executed successfully. {output.rowcount} rows affected.", None

        except Exception as e:
            print(f"Failed to run query {sql_query}, with error: {e}")   
            return None


    def ingest_artifacts(self, collection, isVerbose=False):
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
        # Launch the script to stop mysql server
        stop_mqsql_path = os.path.join(self.current_dir, 'alchemy_utils', 'stop_mysql.sh')
        args = [self.path_to_db_installation]
        subprocess.run([stop_mqsql_path] + args, check=True)

