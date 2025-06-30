from dsi.core import Terminal, Sync
from collections import OrderedDict
import numpy as np
import pandas as pd
import os
import sys
from contextlib import redirect_stdout
import re
import io

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

class DSI():
    '''
    A user-facing interface for DSI's Core middleware.

    The DSI Class abstracts Core.Terminal for managing metadata and Core.Sync for data management and movement.
    '''

    def __init__(self, filename = ".temp.db", backend_name = "Sqlite"):
        """
        Initializes DSI by activating a backend for data operations; default is a Sqlite backend for temporary data analysis.
        If users specify `filename`, data is saved to a permanent backend file.
        Can now call read(), find(), update(), query(), write() or any backend printing operations

        `filename` : str, optional
            If not specified, a temporary, hidden backend file is created for users to analyze their data.
            If specified and backend file already exists, it is activated for a user to explore its data.
            If specified and backend file does not exist, a file with this name is created.
            
            Accepted file extensions:
                - If backend_name = "Sqlite" → .db, .sqlite, .sqlite3
                - If backend_name = "DuckDB" → .duckdb, .db
            
        `backend_name` : str, optional
            Name of the backend to activate. Must be either "Sqlite" or "DuckDB".
            Default is "Sqlite".
        """
        self.t = Terminal(debug = 0, runTable=False)
        self.s = Sync()
        self.t.user_wrapper = True
        self.backend_name = None
        self.schema_read = False
        self.schema_tables = set()
        self.loaded_tables = set()

        if filename == ".temp.db" and os.path.exists(filename):
            os.remove(filename)

        if filename != ".temp.db" and backend_name == "Sqlite":
            file_extension = filename.rsplit(".", 1)[-1] if '.' in filename else ''
            if file_extension.lower() not in ["db", "sqlite", "sqlite3"]:
                filename += ".db"
        elif filename != ".temp.db" and backend_name == "DuckDB":
            file_extension = filename.rsplit(".", 1)[-1] if '.' in filename else ''
            if file_extension.lower() not in ["db", "duckdb"]:
                filename += ".db"
        self.database_name = filename

        fnull = open(os.devnull, 'w')
        try:
            if backend_name.lower() == 'sqlite':
                with redirect_stdout(fnull):
                    self.t.load_module('backend','Sqlite','back-write', filename=filename)
                    self.backend_name = "sqlite"
            elif backend_name.lower() == 'duckdb':
                with redirect_stdout(fnull):
                    self.t.load_module('backend','DuckDB','back-write', filename=filename)
                    self.backend_name = "duckdb"
            else:
                print("Please check the 'backend_name' argument as that one is not supported by DSI")
                print(f"Eligible backend_names are: Sqlite, DuckDB")
        except Exception as e:
            sys.exit(f"backend ERROR: {e}")

        self.main_backend_obj = self.t.loaded_backends[0]
        if filename != ".temp.db":
            print(f"Created an instance of DSI with the {backend_name} backend: {filename}")
        else:
            print("Created an instance of DSI")

    def list_backends(self):
        """
        Prints a list of valid backends that can be used in the `backend_name` argument in `backend()`
        """
        print("\nValid Backends for `backend_name` in backend():\n" + "-" * 40)
        print("Sqlite : Lightweight, file-based SQL backend. Default backend used by DSI API.")
        print("DuckDB : In-process SQL backend optimized for fast analytics on large datasets.\n")
        print()

    def schema(self, filename):
        """
       Loads a relational database schema into DSI from a specified `filename`

        `filename` : str
            Path to a JSON file describing the structure of a relational database.
            The schema should follow the format described in :ref:`user_schema_example_label`

        **Must be called before reading in any data files associated with the schema**
        """
        if not os.path.exists(filename):
            sys.exit("schema() ERROR: Input schema file must have a valid filepath. Please check again.")

        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            self.t.load_module('plugin', 'Schema', 'reader', filename=filename)
        self.schema_read = True
        pk_tables = set(t[0] for t in self.t.active_metadata["dsi_relations"]["primary_key"])
        fk_tables = set(t[0] for t in self.t.active_metadata["dsi_relations"]["foreign_key"] if t[0] != None)
        self.schema_tables = pk_tables.union(fk_tables)
        print(f"Successfully loaded the schema file: {filename}")

    def list_readers(self):
        """
        Prints a list of valid readers that can be used in the `reader_name` argument in `read()`
        """
        print("\nValid Readers for `reader_name` in read():\n" + "-"*50)
        print("CSV                  : Loads data from CSV files (one table per call)")
        print("YAML1                : Loads data from YAML files of a certain structure")
        print("TOML1                : Loads data from TOML files of a certain structure")
        print("JSON                 : Loads single-table data from JSON files")
        print("Ensemble             : Loads a CSV file where each row is a simulation run; creates a simulation table")
        print("Cloverleaf           : Loads data from a directory with subfolders for each simulation run's input and output data")
        print("Bueno                : Loads performance data from Bueno (github.com/lanl/bueno) (.data text file format)")
        print("DublinCoreDatacard   : Loads dataset metadata adhering to the Dublin Core format (XML)")
        print("SchemaOrgDatacard    : Loads dataset metadata adhering to schema.org (JSON)")
        print("GoogleDatacard       : Loads dataset metadata adhering to the Google Data Cards Playbook (YAML)")
        print("Oceans11Datacard     : Loads dataset metadata for Oceans11 DSI data server (oceans11.lanl.gov) (YAML)")
        print()

    def read(self, filenames, reader_name, table_name = None):
        """
        Loads data into DSI using the specified parameter `reader_name`

        `filenames` : str or list of str
            Path(s) to the data file(s) to be loaded.

            The expected file extension depends on the selected `reader_name`:
                - "CSV"                  → .csv
                - "YAML1"                → .yaml or .yml
                - "TOML1"                → .toml
                - "JSON"                 → .json
                - "Ensemble"             → .csv
                - "Cloverleaf"           → /path/to/data/directory/
                - "Bueno"                → .data
                - "DublinCoreDatacard"   → .xml
                - "SchemaOrgDatacard"    → .json
                - "GoogleDatacard"       → .yaml or .yml
                - "Oceans11Datacard"     → .yaml or .yml

        `reader_name` : str
            Name of the DSI reader to use for loading the data. 

            If using a DSI-supported reader, this should be one of the reader_names from `list_readers()`.

            If using a custom reader, provide the relative file path to the Python script with the reader.  
            For guidance on creating a DSI-compatible reader, view :ref:`custom_reader`.

        `table_name` : str, optional
            Name to assign to the loaded table. 
            Only used when the input file contains a single table for the `CSV`, `JSON`, or `Ensemble` reader
        """
        if isinstance(filenames, str) and not os.path.exists(filenames):
            sys.exit("read() ERROR: The input file must be a valid filepath. Please check again.")
        if isinstance(filenames, list) and not all(os.path.exists(f) for f in filenames):
            sys.exit("read() ERROR: All input files must have a valid filepath. Please check again.")
        
        if reader_name.endswith(".py"):
            if not os.path.exists(reader_name):
                sys.exit("read() ERROR: `reader_name` must be a valid filepath to the custom reader. Please check again.")

            import ast
            parsed_data = None
            try:
                with open(reader_name, "r", encoding="utf-8") as external_reader:
                    parsed_data = ast.parse(external_reader.read(), filename=reader_name)
            except Exception as e:
                sys.exit(f"read() Error: Could not read the Python file with the custom reader.")

            class_name = None
            init_params = []
            for node in parsed_data.body:
                if isinstance(node, ast.ClassDef):
                    class_name = node.name
                    functions = {item.name: item.args for item in node.body if isinstance(item, ast.FunctionDef)}

                    if "__init__" in functions and "add_rows" in functions:
                        arg_names = [a.arg for a in functions["__init__"].args]
                        if arg_names and arg_names[0] == "self":
                            arg_names = arg_names[1:]

                        init_params = arg_names + [a.arg for a in functions["__init__"].kwonlyargs]

            if class_name == None:
                sys.exit(f"read() Error: The custom Reader must be structured as a Class in the Python script.")
            elif init_params == []:
                print("read() Error: The custom Reader must be DSI-compatible.")
                sys.exit("Please review https://lanl.github.io/dsi/dev_readers.html to ensure it is compatible.")
            
            updated = {}
            for param in init_params:
                if any(f in param.lower() for f in ["file", "folder", "path", "filename"]):
                    updated[param] = filenames
                if "table" in param.lower():
                    updated[param] = table_name
            
            fnull = open(os.devnull, 'w')
            try:
                with redirect_stdout(fnull):
                    self.t.add_external_python_module('plugin', os.path.splitext(os.path.basename(reader_name))[0], reader_name)
                    self.t.load_module('plugin', class_name, 'reader', **updated)
            except Exception as e:
                sys.exit(f"read() ERROR: {e}")

        else:
            correct_reader = True
            fnull = open(os.devnull, 'w')
            try:
                with redirect_stdout(fnull):
                    if reader_name.lower() == "oceans11datacard":
                        self.t.load_module('plugin', 'Oceans11Datacard', 'reader', filenames=filenames)
                    elif reader_name.lower() == "dublincoredatacard":
                        self.t.load_module('plugin', 'DublinCoreDatacard', 'reader', filenames=filenames)
                    elif reader_name.lower() == "schemaorgdatacard":
                        self.t.load_module('plugin', 'SchemaOrgDatacard', 'reader', filenames=filenames)
                    elif reader_name.lower() == "googledatacard":
                        self.t.load_module('plugin', 'GoogleDatacard', 'reader', filenames=filenames)
                    elif reader_name.lower() == "bueno":
                        self.t.load_module('plugin', 'Bueno', 'reader', filenames=filenames)
                    elif reader_name.lower() == "csv":
                        self.t.load_module('plugin', 'Csv', 'reader', filenames=filenames, table_name = table_name)
                    elif reader_name.lower() == "yaml1":
                        self.t.load_module('plugin', 'YAML1', 'reader', filenames=filenames)
                    elif reader_name.lower() == "toml1":
                        self.t.load_module('plugin', 'TOML1', 'reader', filenames=filenames)
                    elif reader_name.lower() == "ensemble":
                        self.t.load_module('plugin', 'Ensemble', 'reader', filenames=filenames, table_name = table_name)
                    elif reader_name.lower() == "json":
                        self.t.load_module('plugin', 'JSON', 'reader', filenames=filenames, table_name = table_name)
                    elif reader_name.lower() == "cloverleaf":
                        self.t.load_module('plugin', 'Cloverleaf', 'reader', folder_path=filenames)
                    else:
                        correct_reader = False
            except Exception as e:
                sys.exit(f"read() ERROR: {e}")

            if correct_reader == False:
                print("read() ERROR: Please check your spelling of the 'reader_name' argument as it does not exist in DSI\n")
                elg = "CSV, YAML1, TOML1, JSON, Ensemble, Cloverleaf, Bueno, DublinCoreDatacard, SchemaOrgDatacard"
                sys.exit(f"Eligible readers are: {elg}, GoogleDatacard, Oceans11Datacard")

        table_keys = [k for k in self.t.new_tables if k not in ("dsi_relations", "dsi_units")]
        if self.schema_read == True:
            overlap_tables = self.schema_tables & set(self.t.active_metadata.keys())
            if not overlap_tables: # at least one table from schema in the first read()
                sys.exit("read() ERROR: Users must load all associated data for a schema after loading a complex schema.")
            self.loaded_tables.update(overlap_tables)
            
            if self.backend_name == "sqlite":
                try:
                    self.t.artifact_handler(interaction_type='ingest')
                except Exception as e:
                    sys.exit(f"read() ERROR: {e}")
                self.t.active_metadata = OrderedDict()

                # if self.loaded_tables == self.schema_tables:
                self.schema_read = False
                self.schema_tables = set()
                self.loaded_tables = set()

            elif self.backend_name == "duckdb" and self.loaded_tables == self.schema_tables:
                try:
                    self.t.artifact_handler(interaction_type='ingest')
                except Exception as e:
                    sys.exit(f"read() ERROR: {e}")
                self.t.active_metadata = OrderedDict()
                self.schema_read = False
                self.schema_tables = set()
                self.loaded_tables = set()
        else:
            try:
                self.t.artifact_handler(interaction_type='ingest')
            except Exception as e:
                sys.exit(f"read() ERROR: {e}")
            self.t.active_metadata = OrderedDict()

        if len(table_keys) > 1:
            print(f"Loaded {filenames} into tables: {', '.join(table_keys)}")
        else:
            print(f"Loaded {filenames} into the table {table_keys[0]}")

    def query(self, statement, collection = False):
        """
        Executes a SQL query on the active backend.

        `statement` : str
            A SQL query to execute. Only `SELECT` and `PRAGMA` statements are allowed.

        `collection` : bool, optional, default False.
            If True, returns the result as a pandas.DataFrame

            - DataFrame includes 'dsi_table_name' column required for ``dsi.update()``.
              Drop if not using ``update()``.
            
            If False (default), prints the result.

        `return`: If the `statement` is incorrectly formatted, then nothing is returned or printed
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot query() on an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot query() until all associated data is loaded after a complex schema")
        
        output = None
        try:
            f = io.StringIO()
            with redirect_stdout(f):
                df = self.t.artifact_handler(interaction_type='query', query=statement)
            output = f.getvalue()
        except Exception as e:
            sys.exit(f"query() ERROR: {e}")
        if df.empty:
            if output:
                print(output)
            else:
                print("WARNING: input query returned no data. Please check again.")
            return
        if not collection:
            print(f"Printing the result of the SQL query: {statement}")
            headers = df.columns.tolist()
            rows = df.values.tolist()
            self.t.table_print_helper(headers, rows, len(rows))
            print()
        else:
            print(f"Storing the result of the SQL query: {statement} as a collection")
            df.insert(0, "dsi_table_name", self.t.get_table_names(statement)[0])
            print("Note: Includes 'dsi_table_name' column for dsi.update(); DO NOT modify. Drop if not updating data.")
            return df
    
    def get_table(self, table_name, collection = False):
        """
        Retrieves all data from a specified table without requiring knowledge of the active backend's query language.
        
        This method offers a simplified alternative to `query()` for retrieving a full table data without using SQL.

        `table_name` : str
            Name of the table from which all data will be retrieved.

        `collection` : bool, optional, default False.
            If True, returns the result as a pandas.DataFrame

            - DataFrame includes 'dsi_table_name' column required for ``dsi.update()``.
              Drop if not using ``update()``.
            
            If False (default), prints the result.
        
        `return`: If `table_name` does not exist in the backend, then nothing is returned or printed
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot get a table of data from an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot get a table of data until all associated data is loaded after a complex schema")
        
        try:
            df = self.t.get_table(table_name)
        except Exception as e:
            sys.exit(f"get_table() ERROR: {e}")
        if df.empty:
            return
        if not collection:
            print(f"Printing all data from the table: {table_name}")
            headers = df.columns.tolist()
            rows = df.values.tolist()
            self.t.table_print_helper(headers, rows, len(rows))
            print()
        else:
            print(f"Storing all data for the table: {table_name} as a collection")
            df.insert(0, "dsi_table_name", table_name)
            print("Note: Includes 'dsi_table_name' column for dsi.update(); DO NOT modify. Drop if not updating data.")
            return df
        
    def find(self, query, collection = False):
        """
        Finds all rows across all tables in the active backend where `query` can be found.

        If `query` is a string containing a column-level condition (e.g., "age > 4"), this method instead finds all rows 
        in the first table where the condition is satisfied.

        `query` : int, float, or str
            The value to search for in all rows across all tables.

            If `query` is a string with a condition, it must be in the format of a column name, operator, then value.
            Valid operators on numbers or strings:
            
            - age > 4 
            - age < 4 
            - age >= 4 
            - age <= 4 
            - age = 4 
            - age == 4 
            - age != 4 
            - age (4, 8) --> inclusive range between 4 and 8

        `collection` : bool, optional, default False. 
            If True, returns a pandas DataFrame representing a subset of table rows that match or satisfy `query`.

            - DataFrame includes 'dsi_table_name' and 'dsi_row_index' columns required for ``dsi.update()``.
              Drop them if not using ``update()``.

            If False (default), prints the matching rows to the console.

        `return` : If there are no matches found, then nothing is returned or printed
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot find() on an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot find() until all associated data is loaded after a complex schema")
        query = query.replace("\\'", "'") if isinstance(query, str) and "\\'" in query else query
        query = query.replace('\\"', '"') if isinstance(query, str) and '\\"' in query else query
        
        new_find = False
        operators = ['==', '!=', '>=', '<=', '=', '<', '>', '(']
        if isinstance(query, str) and any(op in query for op in operators):
            result = self.t.manual_string_parsing(query)
            if len(result) > 1: # can split into column and operator
                new_find = True
                print(f"Finding all rows where '{query}' in the active backend")

                output = None
                try:
                    f = io.StringIO()
                    with redirect_stdout(f):
                        find_data = self.t.find_relation(query)
                    output = f.getvalue()
                except Exception as e:
                    sys.exit(f"find() ERROR: {e}")
                
                if output and "WARNING" in output:
                    warn_msg = output[output.find("WARNING"):]
                    if "artifact_handler" in warn_msg:
                        lines = warn_msg.splitlines()
                        start = lines[1].find('`')
                        between = lines[1][start + 1 : lines[1].find('`', start + 1)]
                        lines[1] = lines[1].replace(between, "dsi.query()")
                        lines[2] = lines[2].replace(lines[2][lines[2].find('artifact'):-1], "query()")
                        warn_msg = '\n'.join(lines)
                    elif "Could not find" in warn_msg:
                        ending_ind = warn_msg.find("in this database")
                        warn_msg = warn_msg[:40] + query + warn_msg[ending_ind-2:]
                    print("\n"+warn_msg.replace("database", "backend"))
                    return
        
        if new_find == False:
            val = f"'{query}'" if isinstance(query, str) else query
            print(f"Finding all instances of {val} in the active backend")

            fnull = open(os.devnull, 'w')
            try:
                with redirect_stdout(fnull):
                    find_data = self.t.find_cell(query, row=True)
            except Exception as e:
                sys.exit(f"find() ERROR: {e}")
        if find_data is None:
            val = f"'{query}'" if isinstance(query, str) else query
            print(f"WARNING: {val} was not found in this backend\n")
            return
        if collection == False:
            print()
            for val in find_data:
                print(f"Table: {val.t_name}")
                print(f"  - Columns: {val.c_name}")
                print(f"  - Row Number: {val.row_num}")
                print(f"  - Data: {val.value}")
            print()
        else:
            table_name = None
            output_df = None
            row_list = []
            for val in find_data:
                if table_name is None:
                    table_name = val.t_name
                    output_df = pd.DataFrame([val.value], columns=val.c_name)
                    row_list.append(val.row_num)
                elif table_name == val.t_name and val.row_num not in row_list:
                    output_df.loc[len(output_df)] = val.value
                    row_list.append(val.row_num)

            output_df.insert(0, "dsi_row_index", row_list)
            output_df.insert(0, "dsi_table_name", table_name)
            first_msg = "Note: Output includes 2 'dsi_' columns required for dsi.update(). DO NOT modify if updating;"
            print(first_msg, "keep any extra rows blank. Drop if not updating.\n")
            return output_df

    def update(self, collection, backup = False):
        """
        Updates data in one or more tables in the active backend using the provided input. 
        Intended to be used after modifying the output of `find()`, `query()`, or `get_table()`

        `collection` : pandas.DataFrame
            The data used to update a table. 
            DataFrame must include unchanged **`dsi_`** columns from `find()`, `query()` or `get_table()` to successfully update.

            - If a 'query()` DataFrame is the input, the corresponding table in the backend will be completely overwritten.

        `backup` : bool, optional, default False. 
            If True, creates a backup file for the DSI backend before updating its data.

            If False (default), only updates the data.

        - NOTE: Columns from the original table cannot be deleted during update. Only edits or column additions are allowed.
        - NOTE: If a updates affect a user-defined primary key column, row order may change upon reinsertion.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot update() an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot update() until all associated data is loaded after a complex schema")
        print("Updating the active backend with the input collection of data")

        if not isinstance(collection, pd.DataFrame):
            sys.exit("ERROR: update() expects a single DataFrame from find(), query(), or get_table()")
        elif 'dsi_table_name' not in collection.columns:
            sys.exit("update() ERROR: The 'dsi_table_name' column was deleted. Need unchanged column to update() that table")
        elif 'dsi_table_name' not in collection.columns:
            t_col = collection['dsi_table_name']
            if not isinstance(t_col[0], str):
                sys.exit("update() ERROR: The 'dsi_table_name' column must be all strings. Extra rows must be empty.")
            if any(not (t_val in [t_col[0], '']) for t_val in t_col):
                sys.exit("update() ERROR: 'dsi_table_name' column must be unchanged table name. Extra rows must be empty strings.")
            if t_col.replace('', pd.NA).dropna().nunique() > 1:
                sys.exit("update() ERROR: The 'dsi_table_name' column should not be modified.")
        
        table_name = collection['dsi_table_name'][0]
        actual_df = None
        if table_name.lower() in self.t.dsi_tables:
            sys.exit(f"update() ERROR: '{table_name}' is a DSI-read-only table. Cannot update it.")
        if 'dsi_row_index' in collection.columns:
            table_df = collection.copy()

            if any(not (isinstance(row_ind, int) or row_ind == '') for row_ind in table_df['dsi_row_index']):
                sys.exit("update() ERROR: 'dsi_row_index' column must be unchanged row indexes. Extra rows must be empty strings.")
            match = table_df['dsi_row_index'].apply(lambda x: isinstance(x, int)) & (table_df['dsi_table_name'] != table_name)
            empty_match = (table_df['dsi_row_index'] == '') & (table_df['dsi_table_name'] != '')
            if match.any() or empty_match.any():
                sys.exit(f"update() ERROR: Rows in 'dsi_table_name' and 'dsi_row_index' must be '{table_name}' and row index or both be empty.")
            numeric_rows = [int(val) for val in table_df['dsi_row_index'] if val != '']
            if numeric_rows != sorted(numeric_rows) or len(numeric_rows) != len(set(numeric_rows)):
                sys.exit("update() ERROR: 'dsi_row_index' must be unchanged and in increasing order.")

            row_num_list = numeric_rows
            table_df = table_df.drop(columns='dsi_table_name')
            table_df = table_df.drop(columns='dsi_row_index')

            fnull = open(os.devnull, 'w')
            with redirect_stdout(fnull):
                actual_df = self.t.get_table(table_name)
            if actual_df.empty: # dont update if this table doesn't exist
                print(f"WARNING: Cannot update the table '{table_name}' as it does not exist in the active backend.\n")
                return
            
            if len(row_num_list) > len(actual_df) or any(ind > len(actual_df) for ind in row_num_list):
                sys.exit("update() ERROR: 'dsi_row_index' was modified. When adding new rows, values for 'dsi_row_index' must be empty.")

            if not set(actual_df.columns).issubset(set(table_df.columns)):
                sys.exit(f"update() ERROR: {table_name}'s edited data must contain all columns from the original table")
            new_cols = list(set(table_df.columns) - set(actual_df.columns))
            if new_cols:
                for col in new_cols:
                    actual_df[col] = None
                actual_df = actual_df[table_df.columns]
            
            for col in actual_df.columns:
                common_dtype = np.result_type(actual_df[col].dtype, table_df[col].dtype)
                actual_df[col] = actual_df[col].astype(common_dtype)
                table_df[col] = table_df[col].astype(common_dtype)
            
            id_list = [x - 1 for x in row_num_list] # IF ROW NUMBERS ARE 1-INDEXED NOT 0-INDEXED
            table_df_max_len = table_df.iloc[:len(id_list)]
            actual_df.loc[id_list] = table_df_max_len.values
            if len(table_df) > len(id_list):
                extra_rows = table_df.iloc[len(id_list):]
                actual_df = pd.concat([actual_df, extra_rows], ignore_index=True)
        else:
            collection = collection.drop(columns='dsi_table_name')
            actual_df = collection.copy()
        
        try:
            if backup == True:
                extension = self.database_name.rfind('.')
                backup_file = self.database_name[:extension] + ".backup" + self.database_name[extension:]
                print(f"Created backup '{backup_file}' before updating the data.")
            self.t.overwrite_table(table_name, actual_df, backup)
        except Exception as e:
            sys.exit(f"update() ERROR: {e}")
    
    # def nb(self):
    #     """
    #     Generates a Python notebook and stores data from the first activated backend
    #     """
    #     if not self.t.loaded_backends:
    #         raise ValueError("Must load a backend first. Call backend() before this")
        
    #     self.t.artifact_handler(interaction_type="notebook")
    #     print("Notebook .ipynb and .html generated.")

    def list_writers(self):
        """
        Prints a list of valid writers that can be used in the `writer_name` argument in `write()`
        """
        print("\nValid Writers for `writer_name` in write():", self.t.VALID_WRITERS, "\n")
        print("ER_Diagram  : Creates a visual ER diagram image based on all tables in DSI.")
        print("Table_Plot  : Generates a plot of numerical data from a specified table.")
        print("Csv_Writer  : Exports the data of a specified table to a CSV file.")
        print()

    def write(self, filename, writer_name, table_name = None):
        """
        Exports data from the active backend using the specified `writer_name`.

        `filename` : str
            Name of the output file to write.

            Expected file extensions based on `writer_name`:
                - "ER_Diagram"   → .png, .pdf, .jpg, .jpeg
                - "Table_Plot"   → .png, .jpg, .jpeg
                - "Csv_Writer"   → .csv

        `writer_name` : str
            Name of the DSI writer to use. Call `list_writers()` to view all available writers.

        `table_name`: str, optional
            Required when using "Table_Plot" or "Csv_Writer" to specify which table to export.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot write() data from an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot write() until all associated data is loaded after a complex schema")

        try:        
            self.t.artifact_handler(interaction_type='process')
        except Exception as e:
            sys.exit(f"write() ERROR: {e}")
        
        correct_writer = True
        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                if writer_name == "ER_Diagram":
                    self.t.load_module('plugin', 'ER_Diagram', 'writer', filename=filename)
                elif writer_name == "Table_Plot":
                    self.t.load_module('plugin', 'Table_Plot', 'writer', filename=filename, table_name = table_name)
                elif writer_name == "Csv_Writer":
                    self.t.load_module('plugin', 'Csv_Writer', 'writer', filename=filename, table_name = table_name)
                    writer_name = "Csv"
                else:
                    correct_writer = False
        except Exception as e:
            sys.exit(f"write() ERROR: {e}")
        
        if correct_writer == False:
            print("Please check your spelling of the 'writer_name' argument as it does not exist in DSI")
            print(f"Eligible writers are: {self.list_writers()}")
            return
        try:
            self.t.transload()
        except Exception as e:
            sys.exit(f"write() ERROR: {e}")

        self.t.active_metadata = OrderedDict()
        print(f"Successfully wrote to the output file {filename}")
    
    def list(self, collection = False):
        """
        Gets the names and dimensions (rows x columns) of all tables in the active backend.

        `collection` : bool, optional, default False. 
            If True, returns a Python list of all the table names
            
            If False (default), prints each table's name and dimensions to the console.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot list() tables of an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call list() until all associated data is loaded after a complex schema")

        output = None
        try:
            f = io.StringIO()
            with redirect_stdout(f):
                self.t.list()
            output = f.getvalue()
        except Exception as e:
            sys.exit(f"list() ERROR: {e}")
        
        if collection:
            output_list = output.split('\n')
            table_list = [line[7:] for line in output_list if line.startswith('Table: ')]
            return table_list
        else:
            print(output)

    def summary(self, table_name = None, collection = False):
        """
        Prints numerical metadata and (optionally) sample data from tables in the active backend.

        `table_name` : str, optional
            If specified, only the numerical metadata for that table will be printed.
            
            If None (default), metadata for all available tables is printed.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot call summary() on an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call summary() until all associated data is loaded after a complex schema")
        
        output = None
        try:
            f = io.StringIO()
            with redirect_stdout(f):
                summary_df = self.t.summary(table_name, collection)
            output = f.getvalue()
        except Exception as e:
            sys.exit(f"summary() ERROR: {e}")

        if collection:
            return summary_df
        else:
            print(output)
    
    def num_tables(self):
        """
        Prints the number of tables in the active backend.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot call num_tables() on an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call num_tables() until all associated data is loaded after a complex schema")
        try:
            self.t.num_tables()
        except Exception as e:
            sys.exit(f"num_tables() ERROR: {e}")

    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data from a specified table in the active backend.
        
        `table_name` : str
            Name of the table to display.
         
        `num_rows` : int, optional, default=25
            Maximum number of rows to print. If the table contains fewer rows, only those are shown.

        `display_cols` : list of str, optional
            List of specific column names to display from the table. 

            If None (default), all columns are displayed.
        """
        if not self.t.valid_backend(self.main_backend_obj, self.main_backend_obj.__class__.__bases__[0].__name__):
            sys.exit("ERROR: Cannot call display() data from an empty backend. Please ensure there is data in it.")
        if self.schema_read == True:
            sys.exit("ERROR: Cannot display() until all associated data is loaded after a complex schema")
        if isinstance(num_rows, list):
            display_cols = num_rows
            num_rows = 25
        try:
            self.t.display(table_name, num_rows, display_cols)
        except Exception as e:
            sys.exit(f"display() ERROR: {e}")

    def close(self):
        """
        Closes the connection to the active backend and clears all loaded DSI modules.
        """
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            self.t.close()
        print("Closing this instance of DSI()")
    
    #help, edge-finding (find this/that)
    def get(self, dbname):
        pass
    def move(self, filepath):
        pass
    def fetch(self, fname):
        pass
