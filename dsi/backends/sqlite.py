import csv
import sqlite3
import json
import re

from dsi.backends.filesystem import Filesystem

# Declare supported named types for sql

DOUBLE = "DOUBLE"
STRING = "VARCHAR"
FLOAT = "FLOAT"
INT = "INT"
JSON = "TEXT"

# Holds table name and data properties

class DataType:
    name = "TABLENAME" # Note: using the word DEFAULT outputs a syntax error
    properties = {}
    units = {}


# Holds the main data

class Artifact:
    """
        Primary Artifact class that holds database schema in memory.
        An Artifact is a generic construct that defines the schema for metadata that
        defines the tables inside of SQL
    """
    name = "TABLENAME"
    properties = {}


# Main storage class, interfaces with SQL

class Sqlite(Filesystem):
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

    def check_type(self, text):
        """
        Tests input text and returns a predicted compatible SQL Type
        `text`: text string
        `return`: string description of a SQL data type
        """
        try:
            _ = int(text)
            return " INT"
        except ValueError:
            try:
                _ = float(text)
                return " FLOAT"
            except ValueError:
                return " VARCHAR"

    # Creates and adds columns to table and their types.
    # Note 1: 'add column types' to be implemented.
    # Note 2: TABLENAME is the default name for all tables created which might cause issues when creating multiple Sqlite files.
    
    def put_artifact_type(self, types, isVerbose=False):
        """
        Primary class for defining metadata Artifact schema.

        `types`: DataType derived class that defines the string name, properties
                 (named SQL type), and units for each column in the schema.

        `return`: none
        """
        
        col_names = ', '.join(types.properties.keys())
        
        str_query = "CREATE TABLE IF NOT EXISTS {} ({});".format(str(types.name), col_names)

        if isVerbose:
            print(str_query)

        self.cur.execute(str_query)
        self.con.commit()

        self.types = types

    def put_artifacts_t(self, collection, tableName="TABLENAME", isVerbose=False):
        """
        Primary class for insertion of collection of Artifacts metadata into a defined schema, with a table passthrough

        `collection`: A Python Collection of an Artifact derived class that has multiple regular structures of a defined schema,
                     filled with rows to insert.

        `tableName`: A passthrough to define a table and set the name of a table

        `return`: none
        """ 

        # Define table name in local class space
        self.types = DataType()
        self.types.name = tableName
        self.put_artifacts(collection, isVerbose)

    # Adds rows to the columns defined previously
    def put_artifacts(self, collection, isVerbose=False):
        """
        Primary class for insertion of collection of Artifacts metadata into a defined schema

        `collection`: A Python Collection of an Artifact derived class that has multiple regular structures of a defined schema,
                     filled with rows to insert.

        `return`: none
        """
        # Core compatibility name assignment
        artifacts = collection

        types = DataType()
        types.properties = {}

        # Check if this has been defined from helper function
        if self.types != None:
            types.name = self.types.name

        for key in artifacts:
            types.properties[key.replace('-','_minus_')] = artifacts[key]
           
        self.put_artifact_type(types)
        
        col_names = ', '.join(types.properties.keys())
        placeholders = ', '.join('?' * len(types.properties))
        
        str_query = "INSERT INTO {} ({}) VALUES ({});".format(str(types.name), col_names, placeholders)
        
        # col_list helps access the specific keys of the dictionary in the for loop
        col_list = col_names.split(', ')

        # loop through the contents of each column and insert into table as a row
        for ind1 in range(len(types.properties[col_list[0]])):
            vals = []
            for ind2 in range(len(types.properties.keys())):
                vals.append(str(types.properties[col_list[ind2]][ind1]))
            # Make sure this works if types.properties[][] is already a string
            tup_vals = tuple(vals)
            self.cur.execute(str_query,tup_vals)

        if isVerbose:
            print(str_query)

        self.con.commit()
        
        self.types = types

    def put_artifacts_only(self, artifacts, isVerbose=False):
        """
        Function for insertion of Artifact metadata into a defined schema as a Tuple

        `Artifacts`: DataType derived class that has a regular structure of a defined schema,
                     filled with rows to insert.

        `return`: none
        """
        self.types = artifacts

        #self.types already defined previous
        col_names = ', '.join(self.types.properties.keys())
        placeholders = ', '.join('?' * len(self.types.properties))

        str_query = "INSERT INTO {} ({}) VALUES ({});".format(str(self.types.name), col_names, placeholders)
        
        if isVerbose:
            print(str_query)

        # col_list helps access the specific keys of the dictionary in the for loop
        col_list = col_names.split(', ')

        # loop through the contents of each column and insert into table as a row
        for ind1 in range(len(self.types.properties[col_list[0]])):
            vals = []
            for ind2 in range(len(self.types.properties.keys())):
                if len(self.types.properties[col_list[ind2]]) <= ind1:
                    vals.append(str(''))
                    continue
                vals.append(str(self.types.properties[col_list[ind2]][ind1]))
            # Make sure this works if types.properties[][] is already a string
            tup_vals = tuple(vals)
            self.cur.execute(str_query,tup_vals)

        self.con.commit()

     # Adds rows to the columns defined previously
    def put_artifacts_lgcy(self,artifacts, isVerbose=False):
        """
        Legacy function for insertion of artifact metadata into a defined schema

        `artifacts`: data_type derived class that has a regular structure of a defined schema, filled with rows to insert.

        `return`: none
        """
        str_query = "INSERT INTO " + str(self.types.name) + " VALUES ( "
        for key, value in artifacts.properties.items():
            if 'file' in key: # Todo, use this to detect str type
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

    def put_artifacts_json(self, fname, tname, isVerbose=False):
        """
        Function for insertion of Artifact metadata into a defined schema by using a JSON file
        `fname`: filepath to the .json file to be read and inserted into the database

        `tname`: String name of the table to be inserted

        `return`: none
        """

        json_str = None
        try:
            j = open(fname)
            data = json.load(j)
            json_str = json.dumps(data)
            json_str = "'" + json_str + "'"
            j.close()
        except IOError as i:
            print(i)
            return
        except ValueError as v:
            print(v)
            return

        types = DataType()
        types.properties = {}
        types.name = tname
        
        # Check if this has been defined from helper function
        if self.types != None:
            types.name = self.types.name

        col_name = re.sub(r'.json', '', fname)
        col_name = re.sub(r'.*/', '', col_name)
        col_name = "'" + col_name + "'"
        types.properties[col_name] = JSON
           
        self.put_artifact_type(types)
        col_names = ', '.join(types.properties.keys())
        str_query = "INSERT INTO {} ({}) VALUES ({});".format(str(types.name), col_names, json_str)
        if isVerbose:
            print(str_query)

        self.types = types
        self.cur.execute(str_query)
        self.con.commit()

    # Adds columns and rows automaticallly based on a csv file
    #[NOTE 3] This method should be deprecated in favor of put_artifacts.
    def put_artifacts_csv(self, fname, tname, isVerbose=False):
        """
        Function for insertion of Artifact metadata into a defined schema by using a CSV file,
        where the first row of the CSV contains the column names of the schema. Any rows
        thereafter contain data to be inserted. Data types are automatically assigned based on
        typecasting and default to a string type if none can be found.

        `fname`: filepath to the .csv file to be read and inserted into the database

        `tname`: String name of the table to be inserted

        `return`: none
        """
        if isVerbose:
            print("Opening " + fname)

        print('Entering csv method')
        #[BEGIN NOTE 1] This is a csv getter. Does it belong? QW
        with open(fname) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            header = next(csv_reader)

            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    str_query = "CREATE TABLE IF NOT EXISTS " + \
                        str(tname) + " ( "
                    for columnd, columnh in zip(row, header):
                        DataType = self.check_type(columnd)
                        str_query = str_query + \
                            str(columnh) + str(DataType) + ","

                    str_query = str_query.rstrip(',')
                    str_query = str_query + " )"

                    if isVerbose:
                        print(str_query)

                    self.cur.execute(str_query)
                    self.con.commit()
                    line_count += 1
        #[END NOTE 1] QW
        #[BEGIN NOTE 2]  This puts each row into a potentially new table. It does not take existing metadata as input. QW
                str_query = "INSERT INTO " + str(tname) + " VALUES ( "
                for column in row:
                    str_query = str_query + " '" + str(column) + "'"
                    str_query = str_query + ","

                str_query = str_query.rstrip(',')
                str_query = str_query + " )"

                if isVerbose:
                    print(str_query)

                self.cur.execute(str_query)
                self.con.commit()
                line_count += 1

            if isVerbose:
                print("Read " + str(line_count) + " rows.")
      #[END NOTE 2]

    # Returns text list from query
    def get_artifact_list(self, isVerbose=False):
        """
        Function that returns a list of all of the Artifact names (represented as sql tables)

        `return`: list of Artifact names
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
    def get_artifacts(self, query):
        self.get_artifacts_list()

    # Closes connection to server
    def close(self):
        self.con.close()

    # ------- Query related functions -----
    # Raw sql query
    def sqlquery(self, query, isVerbose=False):
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
        self.con.commit()

        if isVerbose:
            print(resout)

        return resout

    # Given an output of a sql query, reformat and write a csv of the subset data
    def export_csv_query(self, query, fname, isVerbose=False):
        """
        Function that outputs a csv file of a return query, given an initial query.

        `query`: raw SQL query to be executed on current table

        `fname`: target filename (including path) that will output the return query as a csv file

        `return`: none
        """
        if isVerbose:
            print(query)
        
        # Parse the table out of the query
        tname = query.split("FROM ",1)[1]
        # Check to see if query is delimited
        if ";" in query:
            tname = tname[:-1]

        # Isolate table name from other commands
        if "WHERE" in tname:
            tname = tname.split("WHERE ",1)[0][:-1]
        
        if isVerbose:
            print("Table: " + tname)

        self.cur = self.con.cursor()
        # Carry out query
        qdata = self.con.execute(query)
        
        # Gather column names
        if "*" in query:
            cdata = self.con.execute(f'PRAGMA table_info({tname});').fetchall()
            cnames = [entry[1] for entry in cdata]
        else:
            cnames = query.split("SELECT ",1)[1]
            cnames = cnames.split("FROM ",1)[0][:-1]
            cnames = cnames.split(',')

        if isVerbose:
            print(cnames)

        with open(fname, "w+") as ocsv:
            csvWriter = csv.writer(ocsv, delimiter=',')
            csvWriter.writerow(cnames)

            for row in qdata:
                print(row)
                csvWriter.writerow(row)

        return 1

    def export_csv(self, rquery, tname, fname, isVerbose=False):
        """
        Function that outputs a csv file of a return query, not the query itself

        `rquery`: return of an already called query output

        `tname`: name of the table for (all) columns to export

        `fname`: target filename (including path) that will output the return query as a csv file

        `return`: none
        """
        if isVerbose:
            print(rquery)

        self.cur = self.con.cursor()
        cdata = self.con.execute(f'PRAGMA table_info({tname});').fetchall()
        cnames = [entry[1] for entry in cdata]
        if isVerbose:
            print(cnames)

        with open(fname, "w+") as ocsv:
            csvWriter = csv.writer(ocsv, delimiter=',')
            csvWriter.writerow(cnames)

            for row in rquery:
                print(row)
                csvWriter.writerow(row)

        return 1
    
    # Query file name
    def query_fname(self, name, isVerbose=False):
        """
        Function that queries filenames within the filesystem metadata store

        `name`: string name of a subsection of a filename to be searched

        `return`: query list of filenames matching `name` string
        """
        table = "filesystem"
        str_query = "SELECT * FROM " + \
            str(table) + " WHERE file LIKE '%" + str(name) + "%'"
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout

    # Query file size

    def query_fsize(self, operator, size, isVerbose=False):
        """
        Function that queries ranges of file sizes within the filesystem metadata store

        `operator`: operator input GT, LT, EQ as a modifier for a filesize search

        `size`: size in bytes

        `return`: query list of filenames matching filesize criteria with modifiers
        """
        str_query = "SELECT * FROM " + \
            str(self.types.name) + " WHERE st_size " + \
            str(operator) + " " + str(size)
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout

    # Query file creation time
    def query_fctime(self, operator, ctime, isVerbose=False):
        """
        Function that queries file creation times within the filesystem metadata store

        `operator`: operator input GT, LT, EQ as a modifier for a creation time search

        `ctime`: creation time in POSIX format, see the utils `dateToPosix` conversion function

        `return`: query list of filenames matching the creation time criteria with modifiers
        """
        str_query = "SELECT * FROM " + \
            str(self.types.name) + " WHERE st_ctime " + \
            str(operator) + " " + str(ctime)
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout
