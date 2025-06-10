import sqlite3
import re
import subprocess
from datetime import datetime
import textwrap
import pandas as pd

from collections import OrderedDict
from dsi.backends.filesystem import Filesystem

# Holds table name and data properties
class DataType:
    name = ""
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties

class Artifact:
    # """
    #     Primary Artifact class that holds database schema in memory.
    #     An Artifact is a generic construct that defines the schema for metadata that
    #     defines the tables inside of SQL
    # """
    name = ""
    properties = {}

class ValueObject:
    """
    Data Structure used when returning search results from ``find``, ``find_table``, ``find_column``, or ``find_cell``

        - t_name: table name 
        - c_name: column name as a list. The length of the list varies based on the find function. 
          Read the description of each one to understand the differences
        - row_num: row number. Is useful when finding a value in ``find_cell`` or ``find`` (which includes results from ``find_cell``)
        - type: type of match for this specific ValueObject. {table, column, range, cell, row}

    """
    t_name = "" # table name
    c_name = [] # column name(s) 
    row_num = None # row number
    value = None # value stored from that match. Ex: table data, col data, cell data etc.
    type = "" #type of match, {table, column, range, cell, row}

    # implement this later once filesystem table incoroporated into dsi
    # filesystem_match = [] #list of all elements in that matching row in filesystem table

# Main storage class, interfaces with SQL
class Sqlite(Filesystem):
    """
    SQLite Filesystem Backend to which a user can ingest/process data, generate a Jupyter notebook, and find occurences of a search term
    """
    runTable = False

    def __init__(self, filename):
        """
        Initializes a SQLite backend with a user inputted filename, and creates other internal variables
        """
        self.filename = filename
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()
        self.runTable = Sqlite.runTable

    def check_type(self, input_list):
        """
        **Internal use only. This function is not intended for direct use by users.**

        Evaluates a list and returns the predicted compatible SQLite Type

        `input_list` : list
            A list of values to analyze for type compatibility.

        `return`: str
            A string representing the inferred SQLite data type for the input list.
        """
        for item in input_list:
            if isinstance(item, int):
                return " INTEGER"
            elif isinstance(item, float):
                return " FLOAT"
            elif isinstance(item, str):
                return " VARCHAR"
        return ""
    
    # OLD NAME OF ingest_table_helper. TO BE DEPRECATED IN FUTURE DSI RELEASE
    def put_artifact_type(self, types, foreign_query = None, isVerbose=False):
        self.ingest_table_helper(types, foreign_query, isVerbose)
        
    def ingest_table_helper(self, types, foreign_query = None, isVerbose=False):
        """
        **Users do not interact with this function and should ignore it. Called within ingest_artifacts()**

        Helper function to create SQLite table based on a passed in schema.

        `types` : DataType
            A DataType-derived object that defines:
                - the table name as a string,
                - table properties as a dictionary mapping column names to data,
                - associated units for each column.

        `foreign_query` : str, optional, default=None
            A valid SQL string specifying foreign key constraints to apply to the table.

        `isVerbose` : bool, optional, default=False
            If True, prints the CREATE TABLE statements for debugging or inspection.
        """
        #checking if extra column needs to be added to a table
        if self.cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{types.name}';").fetchone():
            col_names = types.properties.keys()
            col_info = self.cur.execute(f"PRAGMA table_info({types.name});").fetchall()
            query_cols = [column[1] for column in col_info]
            diff_cols = list(set(col_names) - set(query_cols))
            if len(diff_cols) > 0:
                for col in diff_cols:
                    temp_name = col + self.check_type(types.properties[col])
                    self.cur.execute(f"ALTER TABLE {types.name} ADD COLUMN {temp_name};")
        else:
            sql_cols = ', '.join(types.unit_keys)
            str_query = "CREATE TABLE IF NOT EXISTS {} ({}".format(str(types.name), sql_cols)
            if self.runTable:
                str_query = "CREATE TABLE IF NOT EXISTS {} (run_id INTEGER, {}".format(str(types.name), sql_cols)            
            if foreign_query != None:
                str_query += foreign_query
            if self.runTable:
                str_query += ", FOREIGN KEY (run_id) REFERENCES runTable (run_id)"
            str_query += ");"

            if isVerbose:
                print(str_query)
            self.cur.execute(str_query)
            self.types = types

    # OLD NAME OF ingest_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def put_artifacts(self, collection, isVerbose=False):
        return self.ingest_artifacts(collection, isVerbose)
    
    def ingest_artifacts(self, collection, isVerbose=False):    
        """
        Primary function to ingest a collection of tables into the defined SQLite database.
        
        Creates the auto generated `runTable` if the corresponding flag was set to True when initializing a Core.Terminal
        Also creates a `dsi_units` table if any units are associated with the ingested data values.

        Can only be called if a SQLite database is loaded as a BACK-WRITE backend. 
        (See `core.py` for distinction between BACK-READ and BACK-WRITE.)

        `collection` : OrderedDict
            A nested OrderedDict representing multiple tables and their associated data. 
            Each top-level key is a table name, and its value is an OrderedDict of column names and corresponding data lists.

        `isVerbose` : bool, optional, default=False
            If True, prints all SQL insert statements during the ingest process for debugging or inspection purposes.

        `return`: None on successful ingestion. If an error occurs, returns a tuple in the format of: (ErrorType, error message). 
        Ex: (ValueError, "this is an error")
        """
        artifacts = collection

        # if "dsi_relations" in artifacts.keys():
        #     self.cur.execute("PRAGMA FOREIGN KEYS = ON;")
        #     self.con.commit()
        
        if "runTable" in artifacts.keys():
            self.runTable = False

        if self.runTable:
            runTable_create = "CREATE TABLE IF NOT EXISTS runTable (run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_timestamp TEXT UNIQUE);"
            self.cur.execute(runTable_create)

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
                    types.unit_keys.append(key + self.check_type(tableData[key]) + " PRIMARY KEY")
                else:
                    types.unit_keys.append(key + self.check_type(tableData[key]))
            
            # DEPRECATE IN FUTURE DSI RELEASE FOR NEWER FUNCTION NAME
            # self.put_artifact_type(types, foreign_query)
            self.ingest_table_helper(types, foreign_query)
            
            col_names = ', '.join(types.properties.keys())
            placeholders = ', '.join('?' * len(types.properties))

            str_query = "INSERT INTO "
            if self.runTable:
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
                return (sqlite3.Error, e)
                
            self.types = types #This will only copy the last table from artifacts (collections input)            

        dsi_units_data = self.cur.execute(f"PRAGMA table_info(dsi_units)").fetchall()
        if len(dsi_units_data) == 3 and dsi_units_data[1][1] == "column": # old dsi_units table exists
            self.cur.execute(f'ALTER TABLE dsi_units RENAME COLUMN column TO column_name;') # only commited in later try/catch clause
            
        if "dsi_units" in artifacts.keys():
            create_query = "CREATE TABLE IF NOT EXISTS dsi_units (table_name TEXT, column_name TEXT, unit TEXT)"
            self.cur.execute(create_query)
            units_data = artifacts["dsi_units"]
            for table_val, col_val, unit_val in zip(units_data["table_name"], units_data["column_name"], units_data["unit"]):
                str_query = f'INSERT INTO dsi_units VALUES ("{table_val}", "{col_val}", "{unit_val}")'
                unit_result = self.cur.execute(f"""SELECT unit FROM dsi_units 
                                                WHERE table_name = '{table_val}' AND column_name = '{col_val}';""").fetchone()
                if unit_result and unit_result[0] != unit_val: #checks if unit for same table and col exists in db and if units match
                    self.con.rollback()
                    return (TypeError, f"Cannot ingest different units for the column {col_val} in {table_val}")
                elif not unit_result:
                    try:
                        self.cur.execute(str_query)
                    except sqlite3.Error as e:
                        self.con.rollback()
                        return (sqlite3.Error, e)
                            
        try:
            self.con.commit()
        except Exception as e:
            self.con.rollback()
            return (sqlite3.Error, e)

    # OLD NAME OF query_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def get_artifacts(self, query, isVerbose=False, dict_return = False):
        return self.query_artifacts(query, isVerbose, dict_return)
    
    def query_artifacts(self, query, isVerbose=False, dict_return = False):
        """
        Executes a SQL query on the SQLite backend and returns the result in the specified format dependent on `dict_return`

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
        if query[:6].lower() == "select" or query[:6].lower() == "pragma":
            try:
                data = pd.read_sql_query(query, self.con) 
                if isVerbose:
                    print(data)
            except:
                return (sqlite3.Error, "Error in query_artifacts/get_artifacts: Incorrect SELECT query on the data. Please try again")
        else:
            return (RuntimeError, "Error in query_artifacts/get_artifacts: Can only run SELECT or PRAGMA queries on the data")
        
        if dict_return:
            tables = self.get_table_names(query)
            if len(tables) > 1:
                return (RuntimeError, "Error in query_artifacts/get_artifacts: Can only return ordered dictionary if query with one table")
            
            return OrderedDict(data.to_dict(orient='list'))
        else:
            return data
        
    def get_table(self, table_name, dict_return = False):
        """
        Retrieves all data from a specified table without requiring knowledge of SQL.
        
        This method is a simplified alternative to `query_artifacts()` for users who are only familiar with Python.

        `table_name` : str
            Name of the table in the SQLite backend.

        `dict_return` : bool, optional, default=False
            If True, returns the result as an OrderedDict.
            If False, returns the result as a pandas DataFrame.

        `return` : pandas.DataFrame or OrderedDict or tuple
            - If query is valid and `dict_return` is False: returns a DataFrame.
            - If query is valid and `dict_return` is True: returns an OrderedDict.
            - If query is invalid: returns a tuple (ErrorType, "error message"). Ex: (ValueError, "this is an error")
        """
        return self.query_artifacts(query=f"SELECT * FROM {table_name}", dict_return=dict_return)
    
    def get_table_names(self, query):
        """
        Extracts all table names from a SQL query. Helper function for `query_artifacts()` that users do not need to call

        `query` : str
            A SQL query string, typically passed into `query_artifacts()`.

        `return`: list of str
            List of table names referenced in the query.
        """
        all_names = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', query, re.IGNORECASE)
        tables = [table for from_tbl, join_tbl in all_names if (table := from_tbl or join_tbl)]
        return tables

    # OLD NAME OF notebook(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def inspect_artifacts(self, interactive=False):
        return self.notebook(interactive)
    
    def notebook(self, interactive=False):
        """
        Generates a Jupyter notebook displaying all the data in the SQLite database.

        If multiple tables exist, each is displayed as a separate DataFrame.

        If database has table relations, it is stored as a separate dataframe. 
        If database has a units table, each table's units are stored in its corresponding dataframe `attrs` variable

        `interactive`: default is False. When set to True, creates an interactive Jupyter notebook, otherwise creates an HTML file.

        `return`: None
        """
        import nbconvert as nbc
        import nbformat as nbf
        dsi_relations, dsi_units = None, None

        collection = self.process_artifacts(only_units_relations=True)
        
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

    # OLD NAME OF process_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def read_to_artifact(self, only_units_relations = False):
        return self.process_artifacts(only_units_relations)
    
    def process_artifacts(self, only_units_relations = False):
        """
        Reads data from the SQLite database into a nested OrderedDict.
        Keys are table names, and values are OrderedDicts containing table data.

        If the database contains PK/FK relationships, they are stored in a special `dsi_relations` table.

        `only_units_relations` : bool, default=False
            **USERS SHOULD IGNORE THIS FLAG.** Used internally by sqlite.py.

        `return` : OrderedDict
            A nested OrderedDict containing all data from the SQLite database.
        """
        artifact = OrderedDict()
        artifact["dsi_relations"] = OrderedDict([("primary_key",[]), ("foreign_key", [])])

        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        pkList = []
        for item in tableList:
            tableName = item[0]
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

    def find(self, query_object):
        """
        Searches for all instances of `query_object` in the SQLite database at the table, column, and cell levels. 
        Includes partial matches as well.
        
        `query_object` : int, float, or str
            The value to search for across all tables in the backend.

        `return` : list or tuple
            A list of ValueObjects representing matches. 
            If no matches are found, returns a tuple of an empty ValueObject and an error message.

        - Note: ValueObjects may vary in structure depending on whether the match occurred at the table, column, or cell level.
        - Refer to `find_table()`, `find_column()`, and `find_cell()` for the specific structure of each ValueObject type.
        """
        table_match = self.find_table(query_object)
        col_match = self.find_column(query_object)
        cell_match = self.find_cell(query_object)
        all_return = []
        if isinstance(table_match, list):
            all_return+= table_match
        if isinstance(col_match, list):
            all_return+= col_match
        if isinstance(cell_match, list):
            all_return+= cell_match
        if len(all_return) > 0:
            return all_return
        else:
            return (ValueObject(),  f"{query_object} was not found in this database")
        
    def find_table(self, query_object):
        """
        Finds all tables whose names match or partially match the given `query_object`.

        `query_object` : str
            The string to search for in table names.

        `return` : list of ValueObjects
            One ValueObject per matching table.

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list of all columns in the table
            - value:    table data as list of rows (each row is a list)
            - row_num:  None
            - type:     'table'
        """
        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        tableList = [table[0] for table in tableList]
        if "sqlite_sequence" in tableList:
            tableList.remove("sqlite_sequence")

        if isinstance(query_object, str):
            table_return_list = []
            for table in tableList:
                if query_object in table:
                    colData = self.cur.execute(f"PRAGMA table_info({table});").fetchall()
                    col_names = [column[1] for column in colData]
                    table_data = self.cur.execute(f"SELECT * FROM {table};").fetchall()
                    val = ValueObject()
                    val.t_name = table
                    val.c_name = col_names
                    val.value = table_data
                    val.type = "table"
                    table_return_list.append(val)
            
            if len(table_return_list) > 0:
                return table_return_list
            return (ValueObject(), f"{query_object} is not a table name in this database")
        return (ValueObject(), f"{query_object} needs to be a string if finding among table names")
    
    def find_column(self, query_object, range = False):
        """
        Finds all columns whose names match or partially match the given `query_object`.

        `query_object` : str
            The string to search for in column names.

        `range` : bool, optional, default=False
            If True, `value` in the returned ValueObject will be the [min, max] of the matching numerical column.
            If False, `value` in the returned ValueObject will be the full list of column data.

        `return` : List of ValueObjects if there is a match. 
        
        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list containing one element - the matching column name
            - value:

                - If range=True: [min, max]
                - If range=False: list of column data
            - row_num:  None
            - type:
            
                - If range=True: 'range'
                - If range=False: 'column'
        """
        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        tableList = [table[0] for table in tableList]
        if "sqlite_sequence" in tableList:
            tableList.remove("sqlite_sequence")

        if isinstance(query_object, str):
            col_return_list = []
            for table in tableList:
                colList = self.cur.execute(f"PRAGMA table_info({table});").fetchall()
                for col in colList:
                    if query_object in col[1]:
                        returned_col = self.cur.execute(f"SELECT {col[1]} FROM {table};").fetchall()
                        colData = [row[0] for row in returned_col]
                        not_numeric = any(isinstance(item, str) for item in colData)

                        val = ValueObject()
                        val.t_name = table
                        val.c_name = [col[1]]
                        if range == True and not not_numeric:
                            numeric_col = [0 if item is None else item for item in colData]
                            val.value = [min(numeric_col), max(numeric_col)]
                            val.type = "range"
                            col_return_list.append(val)
                        elif range == False:
                            val.value = colData
                            val.type = "column"
                            col_return_list.append(val)
            
            if len(col_return_list) > 0:
                return col_return_list
            return (ValueObject(), f"{query_object} is not a column name in this database")
        return (ValueObject(), f"{query_object} needs to be a string if finding among column names")

    def find_cell(self, query_object, row = False):
        """
        Finds all cells in the database that match or partially match the given `query_object`.

        `query_object` : int, float, or str
            The value to search for at the cell level, across all tables in the backend.

        `row`: bool, optional, default=False
            If True, `value` in the returned ValueObject will be the entire row where a cell matched.
            If False, `value` in the returned ValueObject will only be the matching cell value.

        `return` : List of ValueObjects if there is a match.

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list of column names. 

                - If row=True: list of all column names in the table
                - If row=False: list with one element - the matched column name
            - value:

                - If row=True: full row of values
                - If row=False: value of the matched cell
            - row_num:  row index of the match
            - type:

                - If row=True: 'row'
                - If row=False: 'cell'
        """
        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        tableList = [table[0] for table in tableList]
        if "sqlite_sequence" in tableList:
            tableList.remove("sqlite_sequence")
                    
        query_list = []
        for table in tableList:
            colList = self.cur.execute(f"PRAGMA table_info({table});").fetchall()
            all_cols = [column[1] for column in colList]
            row_list = []
            for col in colList:
                middle= None
                if row:
                    middle = f'"{all_cols}", *'
                else:
                    middle = f"'{col[1]}', {col[1]}"
                query = f"SELECT '{table}', (SELECT COUNT(*) FROM {table} AS t2 WHERE t2.rowid <= t1.rowid) AS row_num, {middle} FROM {table} AS t1 WHERE "
                # query = f"SELECT '{table}', rowid, {middle} FROM {table} WHERE "
                if isinstance(query_object, str):
                    query += f"{col[1]} LIKE '%{query_object}%'" 
                else:
                    query += f"CAST({col[1]} AS TEXT) LIKE '%{query_object}%'" 
                row_list.append(query)            

            table_row_query = " UNION ".join(row_list) + ";"
            table_row_return = self.cur.execute(table_row_query).fetchall()
            query_list += table_row_return

        if len(query_list) > 0:
            value_obj_list = []
            for value_row in query_list:
                val = ValueObject()
                val.t_name = value_row[0]
                val.row_num = value_row[1]
                val.c_name = [value_row[2]]
                val.value = value_row[3]
                val.type = "cell"
                if row:
                    val.c_name = eval(value_row[2])
                    val.value = list(value_row[3:])
                    val.type = "row"
                value_obj_list.append(val)

            return value_obj_list

        return (ValueObject(), f"{query_object} is not a cell in this database")

    def list(self):
        """
        Return a list of all tables and their dimensions from this SQLite backend
        """
        tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table';").fetchall()
        tableList = [table[0] for table in tableList]
        if "sqlite_sequence" in tableList:
            tableList.remove("sqlite_sequence")
        
        info_list = []
        for table in tableList:
            colList = self.cur.execute(f"PRAGMA table_info({table});").fetchall()
            num_cols = len(colList)
            num_rows = self.cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            info_list.append((table, num_cols, num_rows))
        
        return info_list
    
    def num_tables(self):
        """
        Prints number of tables in this backend
        """
        table_count = self.cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchone()
        if table_count[0] != 1:
            print(f"Database now has {table_count[0]} tables")
        else:
            print(f"Database now has {table_count[0]} table")
    
    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data of a specified table in this SQLite backend.
        
        `table_name` : str
            Name of the table to display.
         
        `num_rows` : int, optional, default=25
            Maximum number of rows to print. If the table contains fewer rows, only those are shown.

        `display_cols` : list of str, optional
            List of specific column names to display from the table. 

            If None (default), all columns are displayed.
        """
        if len(self.cur.execute(f"PRAGMA table_info({table_name})").fetchall()) == 0:
            return (ValueError, f"{table_name} does not exist in this SQLite database")
        if display_cols == None:
            df = pd.read_sql_query(f"SELECT * FROM {table_name};", self.con) 
        else:
            sql_list = ", ".join(display_cols)
            df = pd.read_sql_query(f"SELECT {sql_list} FROM {table_name};", self.con) 
        if num_rows == -101:
            return df
        headers = df.columns.tolist()
        rows = df.values.tolist()

        print("\nTable: " + table_name)
        self.table_print_helper(headers, rows, num_rows)
        print()
    
    def summary(self, table_name = None, num_rows = 0):
        """
        Prints numerical metadata and (optionally) sample data from tables in the first activated backend.

        `table_name` : str, optional
            If specified, only the numerical metadata for that table will be printed.
            
            If None (default), metadata for all available tables is printed.

        `num_rows` : int, optional, default=0
            If greater than 0, prints the first `num_rows` of data for each selected table (depends if `table_name` is specified).

            If 0 (default), only the total number of rows is printed (no row-level data).
        """
        if table_name is None:
            tableList = self.cur.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name != 'sqlite_sequence';").fetchall()
            if "sqlite_sequence" in tableList:
                tableList.remove("sqlite_sequence")

            for table in tableList:
                print(f"\nTable: {table[0]}")
                headers, rows = self.summary_helper(table[0]) 
                self.table_print_helper(headers, rows, 1000)

                if num_rows > 0:
                    df = pd.read_sql_query(f"SELECT * FROM {table[0]};", self.con) 
                    headers = df.columns.tolist()
                    rows = df.values.tolist()
                    self.table_print_helper(headers, rows, num_rows)
                    print()
                else:
                    row_count = self.cur.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
                    print(f"  - num of rows: {row_count}\n")
        else:
            print(f"\nTable: {table_name}")
            headers, rows = self.summary_helper(table_name) 
            self.table_print_helper(headers, rows, 1000)

            if num_rows > 0:
                df = pd.read_sql_query(f"SELECT * FROM {table_name};", self.con) 
                headers = df.columns.tolist()
                rows = df.values.tolist()
                self.table_print_helper(headers, rows, num_rows)
                print()
            else:
                row_count = self.cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  - num of rows: {row_count}\n")

    def summary_helper(self, table_name):
        """
        **Internal use only.**

        Generates and returns summary metadata for a specific table in the SQLite backend.
        """
        col_info = self.cur.execute(f"PRAGMA table_info({table_name})").fetchall()

        numeric_types = {'INTEGER', 'REAL', 'FLOAT', 'NUMERIC', 'DECIMAL', 'DOUBLE'}
        headers = ['column', 'type', 'min', 'max', 'avg', 'std_dev']
        rows = []

        for col in col_info:
            col_name = col[1]
            col_type = col[2].upper()
            is_primary = col[5] > 0
            display_name = f"{col_name}*" if is_primary else col_name

            if any(nt in col_type for nt in numeric_types):
                min_val, max_val, avg_val, std_dev = self.cur.execute(f"""
                    WITH stats AS (
                        SELECT AVG("{col_name}") AS mean
                        FROM {table_name}
                        WHERE "{col_name}" IS NOT NULL
                    )
                    SELECT 
                        MIN("{col_name}"),
                        MAX("{col_name}"),
                        AVG("{col_name}"),
                        CASE 
                            WHEN COUNT("{col_name}") > 1 THEN 
                                sqrt(AVG(("{col_name}" - stats.mean) * ("{col_name}" - stats.mean)))
                            ELSE NULL
                        END AS std_dev
                    FROM {table_name}, stats
                    WHERE "{col_name}" IS NOT NULL
                """).fetchone()
            else:
                min_val = max_val = avg_val = std_dev = None
            
            rows.append([display_name, col_type, min_val, max_val, avg_val, std_dev])

        return headers, rows

    def overwrite_table(self, table_name, collection):
        """
        Overwrites specified table(s) in this SQLite backend using the provided Pandas DataFrame(s).

        If a relational schema has been previously loaded into the backend, it will be reapplied to the table.
        **Note:** This function permanently deletes the existing table and its data, before inserting the new data.

        `table_name` : str or list
            - If str, name of the table to overwrite in the backend.
            - If list, list of all tables to overwrite in the backend

        `collection` : pandas.DataFrame  or list of Pandas.DataFrames
            - If one item, a DataFrame containing the updated data will be written to the table.
            - If a list, all DataFrames with updated data will be written to their own table
        """
        temp_data = OrderedDict()
        if isinstance(table_name, list) and isinstance(collection, list):
            temp_data = self.process_artifacts()

            for name, data in zip(table_name, collection):
                temp_data[name] = OrderedDict(data.to_dict(orient='list'))
        
        elif isinstance(table_name, str) and isinstance(collection, pd.DataFrame):
            temp_data[table_name] = OrderedDict(collection.to_dict(orient='list'))

            if len(self.cur.execute(f"PRAGMA table_info({table_name})").fetchall()) > 0:
                relations = OrderedDict([('primary_key', []), ('foreign_key', [])])
                tableInfo = self.cur.execute(f"PRAGMA table_info({table_name});").fetchall() 
                for colInfo in tableInfo:
                    if colInfo[5] == 1:
                        relations["primary_key"].append((table_name, colInfo[1]))
                        relations["foreign_key"].append((None, None))
                
                fkData = self.cur.execute(f"PRAGMA foreign_key_list({table_name});").fetchall()
                for row in fkData:
                    relations["primary_key"].append((row[2], row[4]))
                    relations["foreign_key"].append((table_name, row[3]))
                
                if len(relations["primary_key"]) > 0:
                    temp_data["dsi_relations"] = relations

                table_name = [table_name]
                collection = [collection]
        else:
            return (TypeError, "inputs to overwrite_table() need to both be a list or (string, Pandas DataFrame).")

        if "dsi_relations" in temp_data.keys():
            for name, data in zip(table_name, collection):
                result = next((pk_tuple[1] for pk_tuple in temp_data["dsi_relations"]["primary_key"] if name in pk_tuple[0]), None)
                if result:
                    new_data = temp_data[name][result]
                    if any(isinstance(x, str) for x in new_data) and any(isinstance(x, (int, float)) for x in new_data):
                        return (TypeError, f"There are mismatched data types in {name}'s primary key column, {result}. Cannot update.")
                    if len(new_data) != len(set(new_data)):
                        return (ValueError, f"{name}'s primary key column, {result}, must have unique data")
                    
                    rows = self.cur.execute(f"SELECT {result} FROM {name}").fetchall()
                    old_data = [row[0] for row in rows]
                    if old_data != new_data:
                        if name == "runTable" and result == "run_id" and len(table_name) == 1:
                            errorMsg = "Cannot only edit runTable's run_id column."
                            return (TypeError, errorMsg + " Need to update the run_id column in other tables to match runTable")
                        if name == "runTable" and result == "run_id" and len(table_name) > 1:
                            for pk, fk in zip(temp_data["dsi_relations"]["primary_key"], temp_data["dsi_relations"]["foreign_key"]):
                                if pk == (name, result) and fk != (None, None):
                                    fk_data = temp_data[fk[0]][fk[1]]
                                    if not all(item in new_data for item in fk_data):
                                        errorMsg = f"Data in table {fk[0]}'s, run_id column must match runTable's edited run_id column."
                                        return(TypeError, errorMsg + f" Please ensure that all rows in {fk[0]} are being updated")
                        print(f"WARNING: The data in {name}'s primary key column was edited which could reorder rows in the table.")
        
        for name in temp_data.keys():
            self.cur.execute(f'DROP TABLE IF EXISTS "{name}";')
            self.con.commit()
        
        temp_runTable_bool = self.runTable
        if temp_runTable_bool == True:
            self.runTable = False

        errorStmt = self.ingest_artifacts(temp_data)

        if temp_runTable_bool == True:
            self.runTable = True
        
        if errorStmt is not None:
            raise errorStmt[0](f"Error updating data in {self.filename} due to {errorStmt[1]}")

    def table_print_helper(self, headers, rows, max_rows=25):
        """
        **Internal use only.**

        Prints table data and metadata in a clean tabular format.
        """
        # Determine max width for each column
        col_widths = [
            max(
                len(str(h)),
                max((len(str(r[i])) for r in rows if i < len(r)), default=0)
            )
            for i, h in enumerate(headers)
        ]

        # Print header
        header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
        print("\n" + header_row)
        print("-" * len(header_row))

        # Print each row
        count = 0
        for row in rows:
            print(" | ".join(
                f"{str(row[i]):<{col_widths[i]}}" for i in range(len(headers)) if i < len(row)
            ))

            count += 1
            if count == max_rows:
                print(f"  ... showing {max_rows} of {len(rows)} rows")
                break
            
    # Closes connection to server
    def close(self):
        """
        Closes the SQLite database's connection.
        """
        self.con.close()

    # OLD FUNCTION TO DEPRECATE
    def put_artifacts_t(self, collection, tableName="TABLENAME", isVerbose=False):
        """
        DSI 1.0 FUNCTIONALITY - DEPRECATING SOON, DO NOT USE
        
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