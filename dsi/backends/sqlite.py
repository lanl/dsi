import csv
import sqlite3
import json
import re
import subprocess
import nbconvert as nbc
import nbformat as nbf
from datetime import datetime
import textwrap

from collections import OrderedDict
from dsi.backends.filesystem import Filesystem

# Declare supported named types for sql

DOUBLE = "DOUBLE"
STRING = "VARCHAR"
FLOAT = "FLOAT"
INT = "INT"
JSON = "TEXT"

# Holds table name and data properties

class DataType:
    name = "" # Note: using the word DEFAULT outputs a syntax error
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties


# Holds the main data

class Artifact:
    """
        Primary Artifact class that holds database schema in memory.
        An Artifact is a generic construct that defines the schema for metadata that
        defines the tables inside of SQL
    """
    name = ""
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

    def __init__(self, filename, run_table = True):
        self.filename = filename
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()
        self.run_flag = run_table

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

    def put_artifact_type(self, types, foreign_query = None, isVerbose=False):
        """
        Primary class for defining metadata Artifact schema.

        `types`: DataType derived class that defines the string name, properties
                 (named SQL type), and units for each column in the schema.

        `return`: none
        """
        col_names = ', '.join(types.unit_keys)
        if self.run_flag:
            str_query = "CREATE TABLE IF NOT EXISTS {} (run_id, {}".format(str(types.name), col_names)
        else:
            str_query = "CREATE TABLE IF NOT EXISTS {} ({}".format(str(types.name), col_names)

        if foreign_query != None:
            str_query += foreign_query
        
        if self.run_flag:
            str_query += ", FOREIGN KEY (run_id) REFERENCES runTable (run_id));"
        else:
            str_query += ");"

        if isVerbose:
            print(str_query)
        self.cur.execute(str_query)

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
        
        insertError = False
        errorString = None
        if self.run_flag:
            runTable_create = "CREATE TABLE IF NOT EXISTS runTable (run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_timestamp TEXT UNIQUE);"
            self.cur.execute(runTable_create)
            self.con.commit()

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            runTable_insert = f"INSERT INTO runTable (run_timestamp) VALUES ('{timestamp}');"
            if insertError == False:
                try:
                    self.cur.execute(runTable_insert)
                except sqlite3.Error as e:
                    if errorString is None:
                        errorString = e
                    insertError = True
                    self.con.rollback()
            else:
                self.con.rollback()
        
        for tableName, tableData in artifacts.items():
            if tableName == "dsi_relations" or tableName == "dsi_units":
                continue

            types = DataType()
            types.properties = {}
            types.unit_keys = []
            types.name = tableName

            foreign_query = ""
            for key in tableData:
                comboTuple = (tableName, key)
                dsi_name = "dsi_relations"
                if dsi_name in artifacts.keys() and comboTuple in artifacts[dsi_name]["foreign_key"]:
                    foreignIndex = artifacts[dsi_name]["foreign_key"].index(comboTuple)
                    foreign_query += f", FOREIGN KEY ({key}) REFERENCES {artifacts[dsi_name]['primary_key'][foreignIndex][0]} ({artifacts[dsi_name]['primary_key'][foreignIndex][1]})"
                
                types.properties[key.replace('-','_minus_')] = tableData[key]

                if dsi_name in artifacts.keys() and comboTuple in artifacts[dsi_name]["primary_key"]:
                    types.unit_keys.append(key + f"{self.check_type(str(tableData[key][0]))} PRIMARY KEY")
                else:
                    types.unit_keys.append(key + self.check_type(str(tableData[key][0])))
            
            if foreign_query != "":
                self.put_artifact_type(types, foreign_query)
            else:
                self.put_artifact_type(types)
        
            col_names = ', '.join(types.properties.keys())
            placeholders = ', '.join('?' * len(types.properties))

            str_query = "INSERT INTO "
            if self.run_flag:
                run_id = self.cur.execute("SELECT run_id FROM runTable ORDER BY run_id DESC LIMIT 1;").fetchone()[0]
                str_query += "{} (run_id, {}) VALUES ({}, {});".format(str(types.name), col_names, run_id, placeholders)
            else:
                str_query += "{} ({}) VALUES ({});".format(str(types.name), col_names, placeholders)
            
            rows = zip(*types.properties.values())
            if insertError == False:
                try:
                    self.cur.executemany(str_query,rows)
                except sqlite3.Error as e:
                    if errorString is None:
                        errorString = e
                    insertError = True
                    self.con.rollback()
            else:
                self.con.rollback()

            if isVerbose:
                print(str_query)        
            self.types = types #This will only copy the last table from artifacts (collections input)            

        if "dsi_units" in artifacts.keys():
            create_query = "CREATE TABLE IF NOT EXISTS dsi_units (table_name TEXT, column_and_unit TEXT UNIQUE)"
            self.cur.execute(create_query)
            for tableName, tableData in artifacts["dsi_units"].items():
                if len(tableData) > 0:
                    for col_unit_pair in tableData:
                        str_query = f'INSERT OR IGNORE INTO dsi_units VALUES ("{tableName}", "{col_unit_pair}")'
                        if insertError == False:
                            try:
                                self.cur.execute(str_query)
                            except sqlite3.Error as e:
                                if errorString is None:
                                    errorString = e
                                insertError = True
                                self.con.rollback()
                        else:
                            self.con.rollback()

        try:
            assert insertError == False
            self.con.commit()
        except Exception as e:
            self.con.rollback()
            if type(e) is AssertionError:
                return f"No data was inserted into {self.filename} due to the error: {errorString}"
            else:
                return f"No data was inserted into {self.filename} due to the error: {e}"

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
    def get_artifact_list(self, query, isVerbose=False):
        """
        Function that returns a list of all of the Artifact names (represented as sql tables)

        `return`: list of Artifact names
        """
        str_query = query
        if isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()

        if isVerbose:
            print(resout)

        return resout

    # Returns reference from query
    def get_artifacts(self, query, isVerbose=False):
        data = self.get_artifact_list(query)
        return data

    def inspect_artifacts(self, collection, interactive=False):
        dsi_relations = dict(collection["dsi_relations"])
        dsi_units = dict(collection["dsi_units"])

        nb = nbf.v4.new_notebook()
        text = """\
        This notebook was auto-generated by a DSI Backend for SQLite.
        Due to the possibility of several tables stored in the DSI abstraction (OrderedDict), the data is stored as several dataframes in a list
        Execute the Jupyter notebook cells below and interact with table_list to explore your data.
        """
        code1 = """\
        import pandas as pd
        import sqlite3
        """
        code2 = f"""\
        conn = {self.con}
        tables = pd.read_sql_query('SELECT name FROM sqlite_master WHERE type="table";', conn)
        dsi_units = {dsi_units}
        dsi_relations = {dsi_relations}
        """
        code3 = """\
        table_list = []
        for table_name in tables['name']:
            if table_name not in ['dsi_relations', 'dsi_units', 'sqlite_sequence']:
                query = 'SELECT * FROM ' + table_name
                df = pd.read_sql_query(query, conn)
                df.attrs['name'] = table_name
                if table_name in dsi_units:
                    df.attrs['units'] = dsi_units[table_name]
                table_list.append(df)
        
        df = pd.DataFrame(dsi_relations)
        df.attrs['name'] = 'dsi_relations'
        table_list.append(df)
        """
        code4 = """\
        for table_df in table_list:
            print(table_df.attrs)
            print(table_df)
            # table_df.info()
            # table_df.describe()
        """

        nb['cells'] = [nbf.v4.new_markdown_cell(text),
                       nbf.v4.new_code_cell(textwrap.dedent(code1)),
                       nbf.v4.new_code_cell(textwrap.dedent(code2)),
                       nbf.v4.new_code_cell(textwrap.dedent(code3)),
                       nbf.v4.new_code_cell(textwrap.dedent(code4))]
        
        fname = 'dsi_sqlite_backend_output.ipynb'
        print('Writing Jupyter notebook...')
        with open(fname, 'w') as fh:
            nbf.write(nb, fh)

        # open the jupyter notebook for static page generation
        with open(fname, 'r', encoding='utf-8') as fh:
            nb_content = nbf.read(fh, as_version=4)
        run_nb = nbc.preprocessors.ExecutePreprocessor(timeout=-1) # No timeout
        run_nb.preprocess(nb_content, {'metadata':{'path':'.'}})

        if interactive:
            print('Opening Jupyter notebook...')
            
            proc = subprocess.run(['jupyter-lab ./dsi_sqlite_backend_output.ipynb'], capture_output=True, shell=True)
            if proc.stderr != b"":
                raise Exception(proc.stderr)
            return proc.stdout.strip().decode("utf-8")
        else:
            # Init HTML exporter
            html_exporter = nbc.HTMLExporter()
            html_content,_ = html_exporter.from_notebook_node(nb_content)
            # Save HTML file
            html_filename = 'dsi_sqlite_backend_output.html'
            with open(html_filename, 'w', encoding='utf-8') as fh:
                fh.write(html_content)

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
    
    # Sqlite reader class
    def read_to_artifact(self):
        artifact = OrderedDict()
        artifact["dsi_relations"] = OrderedDict([("primary_key",[]), ("foreign_key", [])])

        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        pkList = []
        for item in tableList:
            tableName = item[0]
            if tableName == "dsi_units":
                artifact["dsi_units"] = self.read_units_helper()
                continue

            tableInfo = self.cur.execute(f"PRAGMA table_info({tableName});").fetchall()
            colDict = OrderedDict()
            for colInfo in tableInfo:
                colDict[colInfo[1]] = []
                if colInfo[5] == 1:
                    pkList.append((tableName, colInfo[1]))

            data = self.cur.execute(f"SELECT * FROM {tableName};").fetchall()
            for row in data:
                for colName, val in zip(colDict.keys(), row):
                    colDict[colName].append(val)
            artifact[tableName] = colDict

            fkData = self.cur.execute(f"PRAGMA foreign_key_list({tableName});").fetchall()
            for row in fkData:
                artifact["dsi_relations"]["primary_key"].append((row[2], row[4]))
                artifact["dsi_relations"]["foreign_key"].append((tableName, row[3]))
                if (row[2], row[4]) in pkList:
                    pkList.remove((row[2], row[4]))

        for pk_tuple in pkList:
            if pk_tuple not in artifact["dsi_relations"]["primary_key"]:
                artifact["dsi_relations"]["primary_key"].append(pk_tuple)
                artifact["dsi_relations"]["foreign_key"].append(("NULL", "NULL"))
        return artifact
      
    def read_units_helper(self):
        unitsDict = OrderedDict()
        unitsTable = self.cur.execute("SELECT * FROM dsi_units;").fetchall()
        for row in unitsTable:
            tableName = row[0]
            if tableName not in unitsDict.keys():
                unitsDict[tableName] = []
            unitsDict[tableName].append(eval(row[1]))
        return unitsDict

    '''UNUSED QUERY FUNCTIONS'''
    
    # Query file name
    # def query_fname(self, name, isVerbose=False):
    #     """
    #     Function that queries filenames within the filesystem metadata store

    #     `name`: string name of a subsection of a filename to be searched

    #     `return`: query list of filenames matching `name` string
    #     """
    #     table = "filesystem"
    #     str_query = "SELECT * FROM " + \
    #         str(table) + " WHERE file LIKE '%" + str(name) + "%'"
    #     if isVerbose:
    #         print(str_query)

    #     self.cur = self.con.cursor()
    #     self.res = self.cur.execute(str_query)
    #     resout = self.res.fetchall()

    #     if isVerbose:
    #         print(resout)

    #     return resout

    # # Query file size

    # def query_fsize(self, operator, size, isVerbose=False):
    #     """
    #     Function that queries ranges of file sizes within the filesystem metadata store

    #     `operator`: operator input GT, LT, EQ as a modifier for a filesize search

    #     `size`: size in bytes

    #     `return`: query list of filenames matching filesize criteria with modifiers
    #     """
    #     str_query = "SELECT * FROM " + \
    #         str(self.types.name) + " WHERE st_size " + \
    #         str(operator) + " " + str(size)
    #     if isVerbose:
    #         print(str_query)

    #     self.cur = self.con.cursor()
    #     self.res = self.cur.execute(str_query)
    #     resout = self.res.fetchall()

    #     if isVerbose:
    #         print(resout)

    #     return resout

    # # Query file creation time
    # def query_fctime(self, operator, ctime, isVerbose=False):
    #     """
    #     Function that queries file creation times within the filesystem metadata store

    #     `operator`: operator input GT, LT, EQ as a modifier for a creation time search

    #     `ctime`: creation time in POSIX format, see the utils `dateToPosix` conversion function

    #     `return`: query list of filenames matching the creation time criteria with modifiers
    #     """
    #     str_query = "SELECT * FROM " + \
    #         str(self.types.name) + " WHERE st_ctime " + \
    #         str(operator) + " " + str(ctime)
    #     if isVerbose:
    #         print(str_query)

    #     self.cur = self.con.cursor()
    #     self.res = self.cur.execute(str_query)
    #     resout = self.res.fetchall()

    #     if isVerbose:
    #         print(resout)

    #     return resout