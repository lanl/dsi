import sqlite3
import re
import subprocess
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
    name = ""
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties

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
        if self.cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{types.name}';").fetchone():
            col_names = types.properties.keys()
            col_info = self.cur.execute(f"PRAGMA table_info({types.name});").fetchall()
            query_cols = [column[1] for column in col_info]
            diff_cols = list(set(col_names) - set(query_cols))
            if len(diff_cols) > 0:
                for col in diff_cols:
                    temp_name = col + self.check_type(str(types.properties[col][0]))
                    self.cur.execute(f"ALTER TABLE {types.name} ADD COLUMN {temp_name};")
        else:
            sql_cols = ', '.join(types.unit_keys)
            str_query = "CREATE TABLE IF NOT EXISTS {} ({}".format(str(types.name), sql_cols)
            if self.run_flag:
                str_query = "CREATE TABLE IF NOT EXISTS {} (run_id, {}".format(str(types.name), sql_cols)            
            if foreign_query != None:
                str_query += foreign_query
            if self.run_flag:
                str_query += ", FOREIGN KEY (run_id) REFERENCES runTable (run_id)"
            str_query += ");"

            if isVerbose:
                print(str_query)
            self.cur.execute(str_query)
            self.types = types

    #newer name for put_artifacts. eventually delete put_artifacts
    def write_artifacts(self, collection, isVerbose=False):
        return self.put_artifacts(collection, isVerbose)

    def put_artifacts(self, collection, isVerbose=False):
        """
        Primary class for insertion of collection of Artifacts metadata into a defined schema

        `collection`: A Python Collection of an Artifact derived class that has multiple regular structures of a defined schema,
                     filled with rows to insert.
        `return`: none
        """
        artifacts = collection
        
        if self.run_flag:
            runTable_create = "CREATE TABLE IF NOT EXISTS runTable (run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_timestamp TEXT UNIQUE);"
            self.cur.execute(runTable_create)
            self.con.commit()

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            runTable_insert = f"INSERT INTO runTable (run_timestamp) VALUES ('{timestamp}');"
            self.cur.execute(runTable_insert)

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
                    primaryTuple = artifacts[dsi_name]['primary_key'][foreignIndex]
                    foreign_query += f", FOREIGN KEY ({key}) REFERENCES {primaryTuple[0]} ({primaryTuple[1]})"
                
                types.properties[key.replace('-','_minus_')] = tableData[key]
                
                if dsi_name in artifacts.keys() and comboTuple in artifacts[dsi_name]["primary_key"]:
                    types.unit_keys.append(key + f"{self.check_type(str(tableData[key][0]))} PRIMARY KEY")
                else:
                    types.unit_keys.append(key + self.check_type(str(tableData[key][0])))
            
            self.put_artifact_type(types, foreign_query)
            
            col_names = ', '.join(types.properties.keys())
            placeholders = ', '.join('?' * len(types.properties))

            str_query = "INSERT INTO "
            if self.run_flag:
                run_id = self.cur.execute("SELECT run_id FROM runTable ORDER BY run_id DESC LIMIT 1;").fetchone()[0]
                str_query += "{} (run_id, {}) VALUES ({}, {});".format(str(types.name), col_names, run_id, placeholders)
            else:
                str_query += "{} ({}) VALUES ({});".format(str(types.name), col_names, placeholders)
            if isVerbose:
                print(str_query)
            
            rows = zip(*types.properties.values())
            try:
                self.cur.executemany(str_query,rows)
            except sqlite3.Error as e:
                self.con.rollback()
                return e
                
            self.types = types #This will only copy the last table from artifacts (collections input)            

        if "dsi_units" in artifacts.keys():
            create_query = "CREATE TABLE IF NOT EXISTS dsi_units (table_name TEXT, column TEXT UNIQUE, unit TEXT)"
            self.cur.execute(create_query)
            for tableName, tableData in artifacts["dsi_units"].items():
                if len(tableData) > 0:
                    for col, unit in tableData.items():
                        str_query = f'INSERT INTO dsi_units VALUES ("{tableName}", "{col}", "{unit}")'
                        unit_result = self.cur.execute(f"SELECT unit FROM dsi_units WHERE column = '{col}';").fetchone()
                        if unit_result and unit_result[0] != unit:
                            self.con.rollback()
                            return f"Cannot ingest different units for the column {col} in {tableName}"
                        elif not unit_result:
                            try:
                                self.cur.execute(str_query)
                            except sqlite3.Error as e:
                                self.con.rollback()
                                return e
                        
        try:
            self.con.commit()
        except Exception as e:
            self.con.rollback()
            return e

    #newer name for get_artifacts. eventually delete get_artifacts
    def process_artifacts(self, query, isVerbose=False, dict_return = False):
        return self.get_artifacts(query, isVerbose, dict_return)
    
    def get_artifacts(self, query, isVerbose=False, dict_return = False):
        if query[:6].lower() == "select" or query[:6].lower() == "pragma" :
            try:
                data = self.cur.execute(query).fetchall()
                if isVerbose:
                    print(data)
            except:
                raise ValueError("Error in get_artifacts/process_artifacts handler: Incorrect SELECT query on the data. Please try again")
        else:
            raise ValueError("Error in get_artifacts/process_artifacts handler: Can only run SELECT or PRAGMA queries on the data")
        
        if dict_return:
            query_cols = [description[0] for description in self.cur.description]

            tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', query, re.IGNORECASE)
            # table_names = [table[0] or table[1] for table in tables]
            if len(tables) > 1:
                raise ValueError("Error in get_artifacts/process_artifacts handler: Can only return ordered dictionary if query with one table")
            # col_info = self.cur.execute(f"PRAGMA table_info({table_names[0]});").fetchall()
            # complete_cols = [column[1] for column in col_info]
            # if not set(query_cols).issubset(set(complete_cols)):
            #     raise ValueError("Select query cannot create non-table columns when trying to return an ordered dictionary")
            
            queryDict = OrderedDict()
            for row in data:
                for colName, val in zip(query_cols, row):
                    if colName not in queryDict.keys():
                        queryDict[colName] = []
                    queryDict[colName].append(val)
            return queryDict
        else:
            return data

    def inspect_artifacts(self, interactive=False):
        import nbconvert as nbc
        import nbformat as nbf
        dsi_relations, dsi_units = None, None
        collection = self.read_to_artifact(only_units_relations=True)
        if "dsi_relations" in collection.keys():
            dsi_relations = dict(collection["dsi_relations"])
        if "dsi_units" in collection.keys():
            dsi_units = dict(collection["dsi_units"])

        nb = nbf.v4.new_notebook()
        text = """\
        This notebook was auto-generated by a DSI Backend for SQLite.
        Depending on the data, there might be several tables stored in the DSI abstraction (OrderedDict).
        Therefore, the data will be stored as a list of dataframes where each table corresponds to a dataframe.
        Execute the Jupyter notebook cells below and interact with table_list to explore your data.
        """
        code1 = """\
        import pandas as pd
        import sqlite3
        """
        code2 = f"""\
        dbPath = '{self.filename}'
        conn = sqlite3.connect(dbPath)
        tables = pd.read_sql_query('SELECT name FROM sqlite_master WHERE type="table";', conn)
        """
        if dsi_units is not None:
            code2 += f"""dsi_units = {dsi_units}
        """
        if dsi_relations is not None:
            code2 += f"""dsi_relations = {dsi_relations}
        """
            
        code3 = """\
        table_list = []
        for table_name in tables['name']:
            if table_name not in ["""
        if dsi_units is not None:
            code3 += "'dsi_units', "
        if dsi_relations is not None:
            code3 += "'dsi_relations', "
        code3+="""'sqlite_sequence']:
                query = 'SELECT * FROM ' + table_name
                df = pd.read_sql_query(query, conn)
                df.attrs['name'] = table_name
                """
        if dsi_units is not None:
            code3+= """if table_name in dsi_units:
                    df.attrs['units'] = dsi_units[table_name]
                """
        code3+= """table_list.append(df)
        """
        
        if dsi_relations is not None:
            code3+= """
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

    # SQLITE READER FUNCTION
    def read_to_artifact(self, only_units_relations = False):
        artifact = OrderedDict()
        artifact["dsi_relations"] = OrderedDict([("primary_key",[]), ("foreign_key", [])])

        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        pkList = []
        for item in tableList:
            tableName = item[0]
            if tableName == "dsi_units":
                artifact["dsi_units"] = self.read_units_helper()
                continue
            if tableName == "sqlite_sequence":
                continue

            tableInfo = self.cur.execute(f"PRAGMA table_info({tableName});").fetchall()
            colDict = OrderedDict()
            for colInfo in tableInfo:
                colDict[colInfo[1]] = []
                if colInfo[5] == 1:
                    pkList.append((tableName, colInfo[1]))

            if only_units_relations == False:
                data = self.cur.execute(f"SELECT * FROM {tableName};").fetchall()
                for row in data:
                    for colName, val in zip(colDict.keys(), row):
                        if val == "NULL":
                            colDict[colName].append(None)
                        else:
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
                artifact["dsi_relations"]["foreign_key"].append((None, None))

        if len(artifact["dsi_relations"]["primary_key"]) == 0:
            del artifact["dsi_relations"]

        return artifact
      
    def read_units_helper(self):
        unitsDict = OrderedDict()
        unitsTable = self.cur.execute("SELECT * FROM dsi_units;").fetchall()
        for row in unitsTable:
            tableName = row[0]
            if tableName not in unitsDict.keys():
                unitsDict[tableName] = {}
            unitsDict[tableName][row[1]] = row[2]
        return unitsDict
    
    def find(self, query_object, colFlag = False):
        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        
        if isinstance(query_object, str):
            #table name check - returns either list of cols in table or the whole table as the second part of a tuple. First part is the table name
            for table in tableList:
                if query_object in table[0]:
                    if colFlag == True:
                        colData = self.cur.execute(f"PRAGMA table_info({table[0]});").fetchall()
                        return_cols = [column[1] for column in colData]
                        return (table[0], return_cols)
                    else:
                        returned_table = self.cur.execute(f"SELECT * FROM {table[0]};").fetchall()
                        return (table[0], returned_table)
            
            #col name check - ONLY RETURNS FIRST INSTANCE OF COLUMN FOR NOW as a tuple (table name, column)
            for table in tableList:
                colList = self.cur.execute(f"PRAGMA table_info({table[0]});").fetchall()
                for col in colList:
                    if query_object in col[1]:
                        returned_col = self.cur.execute(f"SELECT {col[1]} FROM {table[0]};").fetchall()
                        return (table[0], returned_col)
                    
        #datapoint search - List of all instances where a datapoint exists. ex: [(table1, col2), (table2, col1), (table3, col4)]
        query_list = []
        for table in tableList:
            colList = self.cur.execute(f"PRAGMA table_info({table[0]});").fetchall()
            for col in colList:
                if isinstance(query_object, str):
                    col_query = f"SELECT '{table[0]}', '{col[1]}' FROM {table[0]} WHERE {col[1]} LIKE '%{query_object}%'" 
                else:
                    col_query = f"SELECT '{table[0]}', '{col[1]}' FROM {table[0]} WHERE CAST({col[1]} AS TEXT) LIKE '%{query_object}%'" 
                query_list.append(col_query)
        
        datapoint_query = " UNION ".join(query_list) + ";"
        datapoint_return = self.cur.execute(datapoint_query).fetchall()
        if len(datapoint_return) > 0:
            return datapoint_return

        raise ValueError(f"{query_object} does not exist in this database")

    # Closes connection to server
    def close(self):
        self.con.close()

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