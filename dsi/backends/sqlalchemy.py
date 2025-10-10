import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, TEXT
from sqlalchemy.types import Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT

import random
import socket
import re
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


class SqlAlchemy(Filesystem):
    """
    SQLAlchemy Backend to access databases such as mysql, ...
    """

    def __get_random_free_port(self, start: int = 1024, end: int = 65535) -> int:
        while True:
            port = random.randint(start, end)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('', port))
                    return port  # Found a free one
                except OSError:
                    continue  # Try another port


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
            print("Please make sre that sqlalchemy_dsi_config.yaml is in the same folder as the launch path")
            return

        # initialize some parameters
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        self.path_to_db_installation = sqlalchemy_dsi_config["path_to_db_installation"]
        self.path_to_data = sqlalchemy_dsi_config["path_to_data"]
        self.sql_server_port = self.__get_random_free_port(1024, 65535)
        self.host = "localhost"
        self.database = filename
        self.user = "dsi_user"
        self.dsi_password = "dsi_password"


        # Launch the script to start mysql server
        start_mqsql_path = os.path.join(self.current_dir, 'alchemy_utils', 'start_mysql.sh')
        args = [self.path_to_db_installation, self.path_to_data, str(self.sql_server_port)]
        subprocess.run([start_mqsql_path] + args, check=True)

        time.sleep(10)   # wait for things to happen


        # Launch the script to create DB and user
        create_db_user_path = os.path.join(self.current_dir, 'alchemy_utils', 'create_db_user.sh')
        args = [self.path_to_db_installation, self.database, self.user, self.dsi_password]
        subprocess.run([create_db_user_path] + args, check=True)

        time.sleep(5)   # wait for things to happen

        # Connect to the Server
        url=f"mysql+pymysql://{self.user}:{self.dsi_password}@{self.host}:{self.sql_server_port}/{self.database}"

        try:
            self.engine = sqlalchemy.create_engine(url)

            try:
                self.conn = self.engine.connect()
                result = self.conn.execute(sqlalchemy.text("SELECT NOW();"))
                print("Connection successful. Server time:", result.scalar())
            except Exception as e:
                print("Transaction Error:", e)

        except Exception as e:
            print("Engine connect failed:", e)

    

    def query_artifacts(self, sql_query: str, isVerbose: bool = False, dict_return: bool = False):
        """
        Executes a SQL query on the SQLALchemy backend and returns the result in the specified format dependent on `dict_return`

        `query` : str
            Must be a SELECT or PRAGMA SQL query. Aggregate functions like COUNT are allowed.
            If `dict_return` is True, the query must target a single table and cannot include joins. 

        `isVerbose` : bool, optional, default=False
            If True, prints the SQL SELECT statements being executed.

        `dict_return` : bool, optional, default=False
            If True, returns the result as an OrderedDict.
            If False, returns the result as a pandas DataFrame.
        
        `return` : pandas.DataFrame or OrderedDict or tuple
            - If query is valid and `dict_return` is False: returns a DataFrame.
            - If query is valid and `dict_return` is True: returns an OrderedDict.
            - If query is invalid: returns a tuple (ErrorType, "error message"). Ex: (ValueError, "this is an error")
        """

        try:
            with self.engine.connect() as conn:
                output = conn.execute(sqlalchemy.text(sql_query))
                conn.commit()  # important for DELETE/INSERT/UPDATE

                if output.returns_rows:
                    return output.fetchall(), list(output.keys())
                else:
                    return f"Query executed successfully. {output.rowcount} rows affected.", None

        except Exception as e:
            print(f"Failed to run query {sql_query}, with error: {e}")   
            return None



    def close(self):
        """ Close the database """
        # Launch the script to stop mysql server
        stop_mqsql_path = os.path.join(self.current_dir, 'alchemy_utils', 'stop_mysql.sh')
        args = [self.path_to_db_installation]
        subprocess.run([stop_mqsql_path] + args, check=True)
