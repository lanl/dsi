import duckdb
import re
from datetime import datetime
import pandas as pd

from collections import OrderedDict
from dsi.backends.filesystem import Filesystem

# Holds table name and data properties
class DataType:
    name = ""
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties

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
class DuckDB(Filesystem):
    """
    DuckDB Filesystem Backend to which a user can ingest/process data, generate a Jupyter notebook, and find occurences of a search term
    """
    runTable = False

    def __init__(self, filename):
        """
        Initializes a DuckDB backend with a user inputted filename, and creates other internal variables
        """
        self.filename = filename
        self.con = duckdb.connect(filename)
        self.cur = self.con.cursor()
        self.runTable = DuckDB.runTable

    def check_type(self, input_list):
        """
        **Users should not use this function. Only used by internal DuckDB functions**

        Evaluates a list and returns the predicted compatible DuckDB Type

        `input_list`: list of values to evaluate

        `return`: string description of the list's DuckDB data type
        """
        for item in input_list:
            if isinstance(item, int):
                return " INTEGER"
            elif isinstance(item, float):
                return " FLOAT"
            elif isinstance(item, str):
                return " VARCHAR"
        return " VARCHAR"
    
    # OLD NAME OF ingest_table_helper. TO BE DEPRECATED IN FUTURE DSI RELEASE
    def put_artifact_type(self, types, foreign_query = None, isVerbose=False):
        self.ingest_table_helper(types, foreign_query, isVerbose)
        
    def ingest_table_helper(self, types, foreign_query = None, isVerbose=False):
        """
        **Users do not interact with this function and should ignore it. Called within ingest_artifacts()**

        Helper function to create DuckDB table based on a passed in schema.

        `types`: DataType derived class that defines the string name, properties (dictionary of table names and table data), 
        and units for each column in the schema.

        `foreign_query`: defaut is None. It is a DuckDB string detailing the foreign keys in this table

        `isVerbose`: default is False. Flag to print all create table DuckDB statements

        `return`: none
        """
        #checking if extra column needs to be added to a table
        if self.cur.execute(f"""
                            SELECT table_name FROM information_schema.tables 
                            WHERE table_type = 'BASE TABLE' AND table_name = '{types.name}'
                            """).fetchone():
            col_names = types.properties.keys()
            col_info = self.cur.execute(f"PRAGMA table_info({types.name});").fetchall()
            query_cols = [column[1] for column in col_info]
            diff_cols = list(set(col_names) - set(query_cols))
            if len(diff_cols) > 0:
                for col in diff_cols:
                    temp_name = col + self.check_type(types.properties[col])
                    try:
                        self.cur.execute(f"ALTER TABLE {types.name} ADD COLUMN {temp_name};")
                    except duckdb.Error as e:
                        self.cur.execute("ROLLBACK")
                        self.cur.execute("CHECKPOINT")
                        return (duckdb.Error, e)
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
            try:
                self.cur.execute(str_query)
            except duckdb.Error as e:
                self.cur.execute("ROLLBACK")
                self.cur.execute("CHECKPOINT")
                return (duckdb.Error, e)
            self.types = types


    # OLD NAME OF ingest_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def put_artifacts(self, collection, isVerbose=False):
        return self.ingest_artifacts(collection, isVerbose)
    
    def ingest_artifacts(self, collection, isVerbose=False):    
        """
        Primary function to ingest a collection of tables into the defined DuckDB database.
        
        Creates the auto generated `runTable` if flag set to True when setting up a Core.Terminal workflow
        Creates `dsi_units` table if there are units for ingested data values.

        Can only be called if a DuckDB database is loaded as a BACK-WRITE backend (check ``core.py`` for distinction)

        `collection`: A Python Collection of several tables and their data structured as a nested Ordered Dictionary.

        `isVerbose`: default is False. Flag to print all insert table DuckDB statements

        `return`: None when stable ingesting. When errors occur, returns a tuple of (ErrorType, error message). 
        Ex: (ValueError, "this is an error")
        """
        artifacts = collection

        self.cur.execute("BEGIN TRANSACTION")
        if self.runTable:
            runTable_create = "CREATE TABLE IF NOT EXISTS runTable " \
            "(run_id INTEGER PRIMARY KEY, run_timestamp TEXT UNIQUE);"
            self.cur.execute(runTable_create)

            sequence_run_id = "CREATE SEQUENCE IF NOT EXISTS seq_run_id START 1;"
            self.cur.execute(sequence_run_id)
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            runTable_insert = f"INSERT INTO runTable VALUES (nextval('seq_run_id'), '{timestamp}');"
            self.cur.execute(runTable_insert)

        table_order = []
        if "dsi_relations" in artifacts.keys(): # only do this extra manipulation if using a complex schema
            for (tableName, key) in artifacts["dsi_relations"]["foreign_key"]:
                if tableName == None:
                    continue
                foreignIndex = artifacts["dsi_relations"]["foreign_key"].index((tableName, key))
                primaryTuple = artifacts["dsi_relations"]['primary_key'][foreignIndex]
                
                if tableName in table_order and primaryTuple[0] in table_order:
                    fk_spot = table_order.index(tableName)
                    pk_spot = table_order.index(primaryTuple[0])
                    if fk_spot < pk_spot:
                        table_order[pk_spot] = primaryTuple[0]
                        table_order[fk_spot] = tableName
                if tableName not in table_order:
                    table_order.append(tableName)
                if primaryTuple[0] not in table_order:
                    table_order.insert(table_order.index(tableName), primaryTuple[0])

            difference = [item for item in artifacts.keys() if item not in table_order]
            table_order = difference + table_order
        else:
            table_order = artifacts.keys()

        for tableName in table_order:
            if tableName == "dsi_relations" or tableName == "dsi_units":
                continue

            tableData = artifacts[tableName]

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
            except duckdb.Error as e:
                self.cur.execute("ROLLBACK")
                self.cur.execute("CHECKPOINT")
                return (duckdb.Error, e)
                
            self.types = types #This will only copy the last table from artifacts (collections input)            

        if "dsi_units" in artifacts.keys():
            create_query = "CREATE TABLE IF NOT EXISTS dsi_units (table_name TEXT, column_name TEXT, unit TEXT)"
            self.cur.execute(create_query)
            for tableName, tableData in artifacts["dsi_units"].items():
                if len(tableData) > 0:
                    for col, unit in tableData.items():
                        str_query = f"INSERT INTO dsi_units VALUES ('{tableName}', '{col}', '{unit}')"
                        unit_result = self.cur.execute(f"""
                                                       SELECT unit FROM dsi_units 
                                                       WHERE table_name = '{tableName}' AND column_name = '{col}';""").fetchone()
                        if unit_result and unit_result[0] != unit: #checks if unit for same table and col exists in db and if units match
                            self.con.rollback()
                            return (TypeError, f"Cannot ingest different units for the column {col} in {tableName}")
                        elif not unit_result:
                            try:
                                self.cur.execute(str_query)
                            except duckdb.Error as e:
                                self.cur.execute("ROLLBACK")
                                self.cur.execute("CHECKPOINT")
                                return (duckdb.Error, e)
                            
        try:
            self.cur.execute("COMMIT")
            self.cur.execute("CHECKPOINT")
        except duckdb.Error as e:
            self.cur.execute("ROLLBACK")
            self.cur.execute("CHECKPOINT")
            return (duckdb.Error, e)


    # OLD NAME OF query_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def get_artifacts(self, query, isVerbose=False, dict_return = False):
        return self.query_artifacts(query, isVerbose, dict_return)
    
    def query_artifacts(self, query, isVerbose=False, dict_return = False):
        """
        Function that returns data from a DuckDB database based on a specified SQL query.
        Data returned varies based on the `dict_return` flag explained below.

        `query`: Must be a SELECT or PRAGMA query. If `dict_return` is True, then this can only be a simple query on one table, NO JOINS.
        Query CAN create new aggregate columns such as COUNT to include in the result regardless of `dict_return`.

        `isVerbose`: default is False. Flag to print all Select table DuckDB statements

        `dict_return`: default is False. When set to True, return type is an Ordered Dict of data from the table specified in `query`.
        
        `return`: 

            - When `query` is of correct format and dict_return = False, returns a Pandas dataframe of that table's data
            - When `query` is of correct format and dict_return = True, 
              return an Ordered Dictionary of data for the table specified in `query`
            - When `query` is incorrect, return a tuple of (ErrorType, error message). Ex: (ValueError, "this is an error")
        """
        if query[:6].lower() == "select" or query[:6].lower() == "pragma":
            try:
                data = self.cur.execute(query).fetch_df()
                if isVerbose:
                    print(data)
            except:
                return (ValueError, "Error in query_artifacts handler: Incorrect SELECT query on the data. Please try again")
        else:
            return (ValueError, "Error in query_artifacts handler: Can only run SELECT or PRAGMA queries on the data")
        
        if dict_return:
            tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', query, re.IGNORECASE)
            if len(tables) > 1:
                return (ValueError, "Error in query_artifacts handler: Can only return ordered dictionary if query with one table")
            
            return OrderedDict(data.to_dict(orient='list'))
        else:
            return data
    
    # OLD NAME OF notebook(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def inspect_artifacts(self, interactive=False):
        return self.notebook(interactive)
    
    def notebook(self, interactive=False):
        pass

    # OLD NAME OF process_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def read_to_artifact(self, only_units_relations = False, isVerbose = False):
        return self.process_artifacts(only_units_relations, isVerbose)
    
    def process_artifacts(self, only_units_relations = False, isVerbose = False):
        """
        Reads in data from the DuckDB database into a nested Ordered Dictionary, 
        where keys are table names and values are Ordered Dictionary of table data.
        If there are PK/FK relations in a database it is stored in a table called `dsi_relations`.

        Can only be called if a loaded DuckDB database is a BACK-READ backend (check core.py for distinction)

        `only_units_relations`: default is False. **USERS SHOULD IGNORE THIS FLAG.** Used by an internal sqlite.py function. 

        `isVerbose`: default is False. When set to True, prints all DuckDB queries to select data and store in abstraction

        `return`: Nested Ordered Dictionary of all data from the DuckDB database
        
        """
        artifact = OrderedDict()
        artifact["dsi_relations"] = OrderedDict([("primary_key",[]), ("foreign_key", [])])

        tableList = self.cur.execute("""
                                     SELECT table_name FROM information_schema.tables
                                     WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                     """).fetchall()
        for item in tableList:
            tableName = item[0]
            if tableName == "dsi_units":
                artifact["dsi_units"] = self.process_units_helper()
                continue

            tableInfo = self.cur.execute(f"PRAGMA table_info({tableName});").fetchdf()
            colDict = OrderedDict((col, []) for col in tableInfo['name'])

            if only_units_relations == False:
                data = self.cur.execute(f"SELECT * FROM {tableName};").fetchall()
                for row in data:
                    for colName, val in zip(colDict.keys(), row):
                        if val == "NULL":
                            colDict[colName].append(None)
                        else:
                            colDict[colName].append(val)
                artifact[tableName] = colDict

        fkData = self.cur.execute(f"""
                                  SELECT table_name, constraint_column_names, referenced_table, referenced_column_names
                                  FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY'""").fetchall()
        for row in fkData:
            artifact["dsi_relations"]["primary_key"].append((row[2], row[3][0]))
            artifact["dsi_relations"]["foreign_key"].append((row[0], row[1][0]))
        
        if not fkData:
            pkData = self.cur.execute(f"SELECT * FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY'").fetchall()
            for pk_table, pk_col in pkData:
                artifact["dsi_relations"]["primary_key"].append((pk_table, pk_col[0]))
                artifact["dsi_relations"]["foreign_key"].append((None, None))

        if len(artifact["dsi_relations"]["primary_key"]) == 0:
            del artifact["dsi_relations"]

        return artifact
      
    def process_units_helper(self):
        """
        **Users do not interact with this function and should ignore it. Called within process_artifacts()**

        Helper function to create the DuckDB database's units table as an Ordered Dictionary.
        Only called if `dsi_units` table exists in the database.

        `return`: units table as an Ordered Dictionary
        """
        unitsDict = OrderedDict()
        unitsTable = self.cur.execute("SELECT * FROM dsi_units;").fetchall()
        for row in unitsTable:
            tableName = row[0]
            if tableName not in unitsDict.keys():
                unitsDict[tableName] = {}
            unitsDict[tableName][row[1]] = row[2]
        return unitsDict

    def find(self, query_object):
        """
        Function that finds all instances of a `query_object` in a DuckDB database. 
        This includes any partial hits if `query_object` is part of a table/col/cell
        
        `query_object`: Object to find in this database. Can be of any type (string, float, int).

        `return`: List of ValueObjects if there is a match. Else returns tuple of empty ValueObject() and an error message.

            - Note: Return list can have ValueObjects with different structure 
              due to table/column/cell matches having different `value` variables
            - Refer to other find functions (table, column and cell) to clearly understand each one's ValueObject structure
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
        Function that finds all tables whose name matches the `query_object`. 
        This includes any partial hits if the `query_object` is part of a table name

        `query_object`: Object to find in all table names. HAS TO BE A STRING

        `return`: List of ValueObjects if there is a match. 

        Structure of ValueObjects for this function:
            - t_name:   string of table name
            - c_name:   list of all columns in matching table
            - value:    table's data as a list of lists (each row is a list)
            - row_num:  None
            - type:     'table'
        """
        tableList = self.cur.execute("""
                                     SELECT table_name FROM information_schema.tables
                                     WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                     """).fetchall()
        tableList = [table[0] for table in tableList]

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
        Function that finds all columns whose name matches the `query_object`. 
        This includes any partial hits if the `query_object` is part of a column name

        `query_object`: Object to find in all column names. HAS TO BE A STRING

        `range`: default is False. If True, min/max of a numerical column that matches `query_object` is included, not column data.

        `return`: List of ValueObjects if there is a match. 
        
        **Structure of ValueObjects for this function:**

            - t_name:   string of table name
            - c_name:   list of one, which is the name of the matching column
            - value:

                - range = True, [min, max] of the column
                - range = False, column data as a list
            - row_num:  None
            - type:
            
                - range = True, 'range'
                - range = False, 'column'
        """
        tableList = self.cur.execute("""
                                     SELECT table_name FROM information_schema.tables
                                     WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                     """).fetchall()
        tableList = [table[0] for table in tableList]

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
        Function that finds all cells that match the `query_object`. 
        This includes any partial hits if the `query_object` is part of a cell value

        `query_object`: Object to find in all cells. Can be of any type (string, float, int).

        `row`: default is False. Set to True, if want to return whole row where there is a match between a cell and `query_object`

        `return`: List of ValueObjects if there is a match.

        **Structure of ValueObjects for this function:**

            - t_name:   string of table name
            - c_name:   list of column names. 

                - row = True, list is all columns in this table
                - row = False, list is one item -- column of cell that matched `query_object`
            - value:

                - row = True, list of whole row where a cell matches `query_object`
                - row = False, value of the cell that matches `query_object`
            - row_num:  row number of the cell that matched
            - type:

                - row = True, 'row'
                - row = False, 'cell'
        """
        tableList = self.cur.execute("""
                                     SELECT table_name FROM information_schema.tables
                                     WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                     """).fetchall()
        tableList = [table[0] for table in tableList]
                    
        query_list = []
        for table in tableList:
            colList = self.cur.execute(f"PRAGMA table_info({table});").fetchall()
            all_cols = [column[1] for column in colList]
            row_list = []
            for col in colList:
                middle= None
                if row:
                    result = ', '.join(str(i) for i in all_cols)
                    middle = f"'{result}', *"
                else:
                    middle = f"'{col[1]}', {col[1]}"
                query = f"""
                        SELECT '{table}', row_number() OVER () AS row_number, {middle} 
                        FROM {table} 
                        WHERE CAST({col[1]} AS TEXT) ILIKE '%{query_object}%'
                        """
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
                    val.c_name = value_row[2].split(', ')
                    val.value = list(value_row[3:])
                    val.type = "row"
                value_obj_list.append(val)
            return value_obj_list

        return (ValueObject(), f"{query_object} is not a cell in this database")

    def list(self):
        """
        Return a list of all tables and their dimensions from this DuckDB backend
        """
        tableList = self.cur.execute("""
                                     SELECT table_name FROM information_schema.tables
                                     WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                     """).fetchall()
        tableList = [table[0] for table in tableList]
        
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
        table_count = self.cur.execute("""
                                       SELECT COUNT(*) 
                                       FROM information_schema.tables 
                                       WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                                       """).fetchone()[0]
        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")
    
    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data of a specified table from this DuckDB backend.
        
        `table_name`: table whose data is printed
         
        `num_rows`: Optional numerical parameter limiting how many rows are printed. Default is 25.

        `display_cols`: Optional parameter specifying which columns in `table_name` to display. Must be a Python list object
        """
        if self.cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] == 0:
            return (ValueError, f"{table_name} does not exist in this DuckDB database")
        if display_cols == None:
            df = self.cur.execute(f"SELECT * FROM {table_name};").fetchdf()
        else:
            sql_list = ", ".join(display_cols)
            df = self.cur.execute(f"SELECT {sql_list} FROM {table_name};").fetchdf()
        if num_rows == -101:
            return df
        headers = df.columns.tolist()
        rows = df.values.tolist()
        
        print("\nTable: " + table_name)
        self.table_print_helper(headers, rows, num_rows)
        print()
    
    def summary(self, table_name = None, num_rows = 0):
        """
        Prints data and numerical metadata of tables from this DuckDB backend. Output varies depending on parameters

        `table_name`: default is None. When specified only that table's numerical metadata is printed. 
        Otherwise every table's numerical metdata is printed

        `num_rows`: default is 0. When specified, data from the first N rows of a table are printed. 
        Otherwise, only the total number of rows of a table are printed. 
        The tables whose data is printed depends on the `table_name` parameter.

        """
        if table_name is None:
            tableList = self.cur.execute("""
                                        SELECT table_name FROM information_schema.tables
                                        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                                        """).fetchall()

            for table in tableList:
                print(f"\nTable: {table[0]}")
                headers, rows = self.summary_helper(table[0]) 
                self.table_print_helper(headers, rows, 1000)

                if num_rows > 0:
                    df = self.cur.execute(f"SELECT * FROM {table[0]};").fetchdf()
                    headers = df.columns.tolist()
                    rows = df.values.tolist()
                    self.table_print_helper(headers, rows, num_rows)
                    print()
                else:
                    row_count = self.cur.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
                    print(f"  - num of rows: {row_count}\n")
        else:
            headers, rows = self.summary_helper(table_name) 
            self.table_print_helper(headers, rows, 1000)

            if num_rows > 0:
                df = self.cur.execute(f"SELECT * FROM {table_name};").fetchdf()
                headers = df.columns.tolist()
                rows = df.values.tolist()
                self.table_print_helper(headers, rows, num_rows)
                print()
            else:
                row_count = self.cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  - num of rows: {row_count}\n")

    
    def summary_helper(self, table_name):
        """
        **Users should not call this function**

        Helper function to generate the summary of tables in this DuckDB database. 
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
                    SELECT
                        MIN("{col_name}"),
                        MAX("{col_name}"),
                        AVG("{col_name}"),
                        STDDEV_SAMP("{col_name}")
                    FROM {table_name}
                    WHERE "{col_name}" IS NOT NULL
                """).fetchone()
            else:
                min_val = max_val = avg_val = std_dev = None
            
            rows.append([display_name, col_type, min_val, max_val, avg_val, std_dev])

        return headers, rows
    
    def table_print_helper(self, headers, rows, max_rows=25):
        """
        **Users should not call this function**

        Helper function to print table data/metdata cleanly
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

    def drop_table(self, table_name):
        self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.con.commit()

    # Closes connection to server
    def close(self):
        """
        Closes the DuckDB database's connection.

        `return`: None
        """
        self.con.close()