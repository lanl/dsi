# Copyright (c) 2024 Mengjiao Han
#
# -*- coding:utf-8 -*-
# @Script: database.py
# @Author: Mengjiao Han
# @Email: mhan@lanl.gov
# @Create At: 2024-10-25 14:52:32
# @Last Modified By: Mengjiao Han
# @Last Modified At: 2024-10-25 19:24:34
# @Description: functions to create, insert, update database 

import sqlite3
import os
import sys

class Database:
    """
    Create a database with filename 
    """
    def __init__(self, filename):
        if(os.path.splitext(filename)[1] != ".db"):
            sys.exit("Invalid file extension. Expected '.db'")
        self.filename = filename
        conn = sqlite3.connect(self.filename)
        conn.close()
    """
    Create a table in the database 
        input:
            - table_name: name of the table, has to be unique between tables 
            - table_header: a list of string represents the table header 
            - header_types: a list of string represents the header type for each table header 
            - primary_keys: a list of string represents which header is the primary keys
            - foreign_keys: create foreign keys refer to keys in other tables. 
            
    """   
    def createTableDefault(self, table_name:str, table_headers:list[str], header_types:list[str], primary_keys:list[str], foreign_keys:= []):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone() is None:
            # If the table doesn't exist, create it
            # creationText = f"CREATE TABLE {table_name} ({', '.join(table_headers)}, PRIMARY KEY ({', '.join(primary_keys)}),"
            creationText = f"CREATE TABLE {table_name} ("
            for h, header in enumerate(table_headers):
                creationText += f"{header} {header_types[h]}, "
            creationText += f"PRIMARY KEY ({', '.join(primary_keys)}),"
            if(len(foreign_keys) != 0):
                # If need to add forengn keys 
                for f, foreign_key in enumerate(foreign_keys):
                    creationText += f"FOREIGN KEY ({foreign_key[0]}) REFERENCES {foreign_key[1]} ({foreign_key[0]}),"
            creationText = creationText[:-1]
            creationText += ");"
            print("creation Text:", creationText)
            cursor.execute(creationText)
            print(f"Table '{table_name}' created.")    
        else:
            print(f"Table '{table_name}' already exists.")

    def insertValueToTable(self, table_name, values):
        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()
        for value in values:
            columns = ', '.join(value.keys())
            placeholders = ', '.join('?' * len(value))
            sql = 'INSERT OR REPLACE INTO {} ({}) VALUES ({})'.format(table_name, columns, placeholders)
            v_cur = [x for x in value.values()]
            # if(table_name == 'halos'):
            #     print(v_cur)
            #     print(sql)
            cursor.execute(sql, v_cur)
        conn.commit()
        conn.close()
    
    def get_table_info(self, table_name: str):
        """
        Get information about a table in the database

        Args:
            table_name (str): name of the table to get infomation about

        Returns:
            int : number of rows in the table
        """

        conn = sqlite3.connect(self.filename)
        cursor = conn.cursor()


        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if cursor.fetchone() is None:
            print(f"Table '{table_name}' does not exist.")
            conn.close()
            return None

        # Get the schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()

        print(f"Schema of the '{table_name}' table:")
        for column in schema:
            print(f"Column ID: {column[0]}, Name: {column[1]}, Type: {column[2]}, "
                f"Not Null: {column[3]}, Default Value: {column[4]}, Primary Key: {column[5]}")
        
        # Get the number of rows with doata
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        n = cursor.fetchone()[0]

        conn.close()

        return n
    
    def queryTable(self, query: str):
        """
        Query the table

        Args:
            query (str): SQL query command 

        Returns:
            answers: 
        """
        conn = sqlite3.connect(self.filename)
        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        cursor.execute(query)
        answers = cursor.fetchall()
        
        return answers