#!/user/bin/python
"""
dsi.sql is the abstraction layer that enables a sqlite backend database for metadata. 
Sets of generic helper functions are included throughout this dsi module.

`isVerbose`: boolean if true enables printing of raw SQL queries alongside helpful logging outputs
                when helper functions are called

"""
import sys
import os
import sqlite3
import csv

isVerbose = 0

class SQLstore(object):
    """ 
    Class that drives Sql query interface with DSI workflow 
    This class will declare store types and SQL initialization
    and interface
    """

    filename = "database.db"
    types = None
    con = None
    cur = None

    def __init__(self, filename):
        """
        Initiliazation function that establishes connection to a SQLite database.

        `filename`: file path to the location of the sqlite database on disk.

        `return`: none
        """
        self.filename = filename
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()

    # Adds columns to table and their types
    def put_artifact_type(self,types):
        """
        Primary function for defining metadata artifact schema.

        `types`: data_type derived class that defines the string name, properties (named SQL type), and units for each column in the schema

        `return`: none
        """
        str_query = "CREATE TABLE IF NOT EXISTS " + str(types.name) + " ( "
        for key, value in types.properties.items():
            str_query = str_query + str(key) + " " + str(value)
            str_query = str_query +  ","

        str_query = str_query.rstrip(',')
        str_query = str_query + " )"

        if isVerbose:
            print(str_query)
        
        self.cur.execute(str_query)
        self.con.commit()

        self.types = types

    # Adds rows to the columns defined previously
    def put_artifacts(self,artifacts):
        """
        Primary function for insertion of artifact metadata into a defined schema

        `artifacts`: data_type derived class that has a regular structure of a defined schema, filled with rows to insert.

        `return`: none
        """
        str_query = "INSERT INTO " + str(self.types.name) + " VALUES ( "
        for key, value in artifacts.properties.items():
            if key == 'file': # Todo, use this to detect str type
                str_query = str_query + " '" + str(value) +"' "
            else:
                str_query = str_query + " " + str(value)

            str_query = str_query +  ","

        str_query = str_query.rstrip(',')
        str_query = str_query + " )"

        if isVerbose:
            print(str_query)
        
        self.cur.execute(str_query)
        self.con.commit()

    # Adds columns and rows automaticallly based on a csv file
    def put_artifacts_csv(self, fname, tname):
        """
        Function for insertion of artifact metadata into a defined schema by using a CSV file, where the first row of the CSV
        contains the column names of the schema. Any rows thereafter contain data to be inserted.

        `fname`: filepath to the .csv file to be read and inserted into the database

        `tname`: String name of the table to be inserted

        `return`: none
        """
        if isVerbose:
            print("Opening " + fname)

        with open(fname) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    str_query = "CREATE TABLE IF NOT EXISTS " + str(tname) + " ( "
                    for column in row:
                        str_query = str_query + str(column) + " VARCHAR "
                        str_query = str_query +  ","

                    str_query = str_query.rstrip(',')
                    str_query = str_query + " )"

                    if isVerbose:
                        print(str_query)
                    
                    self.cur.execute(str_query)
                    self.con.commit()
                    line_count += 1
                else:
                    str_query = "INSERT INTO " + str(tname) + " VALUES ( "
                    for column in row:
                        str_query = str_query + " '" + str(column) + "'"
                        str_query = str_query +  ","

                    str_query = str_query.rstrip(',')
                    str_query = str_query + " )"

                    if isVerbose:
                        print(str_query)
                    
                    self.cur.execute(str_query)
                    self.con.commit()
                    line_count += 1

            if isVerbose:        
                print("Read " + str(line_count) + " rows.")

    # Returns results from query
    def get_artifacts(self,query):
        """
        Sample function to query contents of the database.
        """
        print("test")

    # Closes connection to server
    def close(self):
        """
        Function to manually close the connection to the sqlite server.
        """
        self.con.close()