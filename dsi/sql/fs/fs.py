"""
© 2023. Triad National Security, LLC. All rights reserved.
This program was produced under U.S. Government contract 89233218CNA000001 for Los Alamos
National Laboratory (LANL), which is operated by Triad National Security, LLC for the U.S.
Department of Energy/National Nuclear Security Administration. All rights in the program are
reserved by Triad National Security, LLC, and the U.S. Department of Energy/National Nuclear
Security Administration. The Government is granted for itself and others acting on its behalf a
nonexclusive, paid-up, irrevocable worldwide license in this material to reproduce, prepare
derivative works, distribute copies to the public, perform publicly and display publicly, and to permit
others to do so.

**dsi.sql.fs** is the filesystem abstraction layer, derived from the sql class that enables 
filesystem related metadata gathering into a sqlite backend database. Specialized
helper functions are included relating to filesystem specific metadata

  `isVerbose`: boolean if true enables printing of raw SQL queries alongside helpful logging outputs
                when helper functions are called

"""
import os
import sqlite3
import csv

from dsi.utils import utils

isVerbose = 0
#class fs:
#    name = ""
#    properties = {}
#    def greet(self):
#        print('Hello')

# Declare named types for sql
DOUBLE = "DOUBLE"
STRING = "VARCHAR"
FLOAT = "FLOAT"
INT = "INT"

# Declare comparison types for sql
GT = ">"
LT = "<"
EQ = "="

# Holds table name and data properties 
class data_type:
    name = "DEFAULT"
    properties = {}
    units = {}

# Holds the main data
class artifact:
    """
        Primary artifact class that holds database schema in memory. 
        An artifact is a generic construct that defines the schema for metadata that
        defines the tables inside of SQL
    """
    properties = {}

# Main storage class, interfaces with SQL
class store:
    """
        Primary storage class, inherits sql class
    """
    filename = "fs.db"
    types = None
    con = None
    cur = None

    def __init__(self, filename):
        self.filename = filename
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()

    # Adds columns to table and their types
    def put_artifact_type(self,types):
        """
        Primary class for defining metadata artifact schema.

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
        Primary class for insertion of artifact metadata into a defined schema

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
        contains the column names of the schema. Any rows thereafter contain data to be inserted. Data types are automatically
        assigned based on typecasting and default to a string type if none can be found.

        `fname`: filepath to the .csv file to be read and inserted into the database

        `tname`: String name of the table to be inserted

        `return`: none
        """
        if isVerbose:
            print("Opening " + fname)

        with open(fname) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            header = next(csv_reader)

            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    str_query = "CREATE TABLE IF NOT EXISTS " + str(tname) + " ( "
                    for columnd,columnh in zip(row,header):
                        data_type = utils.check_type(columnd)
                        str_query = str_query + str(columnh) + str(data_type) + ","

                    str_query = str_query.rstrip(',')
                    str_query = str_query + " )"

                    if isVerbose:
                        print(str_query)
                    
                    self.cur.execute(str_query)
                    self.con.commit()
                    line_count += 1

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

    # Returns text list from query
    def get_artifact_list(self):
        """
        Function that returns a list of all of the artifact names (represented as sql tables)

        `return`: list of artifact names
        """
        str_query = "SELECT name FROM sqlite_master WHERE type='table';"
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout

    # Returns reference from query
    def get_artifacts(self,query):
        print("test")

    # Closes connection to server
    def close(self):
        self.con.close()

    # ------- Query related functions -----
    # Raw sql query
    def sqlquery(self,query):
        """
        Function that provides a direct sql query passthrough to the database.

        `query`: raw SQL query to be executed on current table

        `return`: raw sql query list that contains result of the original query
        """
        if isVerbose:
            print(query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout

    # Given an output of a sql query, reformat and write a csv of the subset data
    def export_csv(self,query,fname):
        """
        Function that outputs a csv file of a return query, given an initial query.

        `query`: raw SQL query to be executed on current table

        `fname`: target filename (including path) that will output the return query as a csv file

        `return`: none
        """
        if isVerbose:
            print(query)

        tname = "vision"
        self.cur = self.con.cursor()
        cdata = self.con.execute(f'PRAGMA table_info({tname});').fetchall()
        cnames = [entry[1] for entry in cdata]
        if isVerbose:
            print(cnames)

        with open(fname,"w+") as ocsv:
            csvWriter = csv.writer(ocsv,delimiter=',')
            csvWriter.writerow(cnames)

            for row in query:
                print(row)
                csvWriter.writerow(row)
        
        return 1

    # Query file name
    def query_fname(self, name ):
        """
        Function that queries filenames within the filesystem metadata store

        `name`: string name of a subsection of a filename to be searched

        `return`: query list of filenames matching `name` string
        """
        table = "filesystem"
        str_query = "SELECT * FROM " + str(table) + " WHERE file LIKE '%" + str(name) +"%'"
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if isVerbose:
            print(resout)

        return resout


    # Query file size
    def query_fsize(self, operator, size ):
        """
        Function that queries ranges of file sizes within the filesystem metadata store

        `operator`: operator input GT, LT, EQ as a modifier for a filesize search

        `size`: size in bytes

        `return`: query list of filenames matching filesize criteria with modifiers
        """
        str_query = "SELECT * FROM " + str(self.types.name) + " WHERE st_size " + str(operator) + " " + str(size)
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if isVerbose:
            print(resout)

        return resout

    # Query file creation time
    def query_fctime(self, operator, ctime ):
        """
        Function that queries file creation times within the filesystem metadata store

        `operator`: operator input GT, LT, EQ as a modifier for a creation time search

        `ctime`: creation time in POSIX format, see the utils `dateToPosix` conversion function

        `return`: query list of filenames matching the creation time criteria with modifiers
        """
        str_query = "SELECT * FROM " + str(self.types.name) + " WHERE st_ctime " + str(operator) + " " + str(ctime)
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if isVerbose:
            print(resout)

        return resout

        
