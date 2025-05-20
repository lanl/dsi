from dsi.core import Terminal, Sync, FindObject
from collections import OrderedDict
import numpy as np
import pandas as pd
import re

class DSI():
    '''
    A user-facing interface for DSI's Core middleware.

    The DSI Class abstracts Core.Terminal for managing metadata and Core.Sync for data management and movement.
    '''

    def __init__(self):
        self.t = Terminal(debug = 0, runTable=False)
        self.s = Sync()

    def list_backends(self):
        """
        Prints a list of valid backends that can be used in the `backend_name` argument in `backend()`
        """
        print("\nValid Backends for `backend_name` in backend():\n" + "-" * 40)
        print("Sqlite : Lightweight, file-based SQL backend. Default backend used by DSI API.")
        print("DuckDB : In-process SQL backend optimized for fast analytics on large datasets.\n")
        print()

    def backend(self, filename, backend_name = "Sqlite"):
        """
        Activates a backend for data operations; default is Sqlite unless user specifies. 
        Can now call read(), find(), query(), write() or any backend printing operations

        `filename` : str
            Name of the backend file to create.
            
            Accepted file extensions:
                - If backend_name = "Sqlite" → .db, .sqlite, .sqlite3
                - If backend_name = "DuckDB" → .duckdb, .db
            
        `backend_name` : str, optional
            Name of the backend to activate. Must be either "Sqlite" or "DuckDB".
            Default is "Sqlite".
        """
        if backend_name.lower() == 'sqlite':
            self.t.load_module('backend','Sqlite','back-write', filename=filename)
        elif backend_name.lower() == 'duckdb':
            self.t.load_module('backend','DuckDB','back-write', filename=filename)
        else:
            print("Please check the 'backend_name' argument as that is not supported by DSI now")
            print(f"Eligible backend_names are: Sqlite, DuckDB")

    def schema(self, filename):
        """
       Loads a relational database schema into DSI from a specified `filename`

        `filename` : str
        Path to a JSON file describing the structure of a relational database.
        The schema should follow the format defined in `examples/data/example_schema.json`.

        **Must be called before reading in any data files associated with the schema**
        """
        self.t.load_module('plugin', 'Schema', 'reader', filename=filename)

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
                - "Bueno"                → .data
                - "DublinCoreDatacard"   → .xml
                - "SchemaOrgDatacard"    → .json
                - "GoogleDatacard"       → .yaml or .yml
                - "Oceans11Datacard"     → .yaml or .yml

        `reader_name` : str
            Name of the DSI reader to use for loading the data. 
            Call `list_readers()` to see a list of supported reader names.

        `table_name` : str, optional
            Name to assign to the loaded table. 
            Only used when the input file contains a single table for the `CSV`, `JSON`, or `Ensemble` reader
        """
        if not self.t.loaded_backends:
            raise ValueError("Must load a backend first. Call backend() before this")
        
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
        else:
            print("Please check your spelling of the 'reader_name' argument as it does not exist in DSI\n")
            print("Eligible readers are: CSV, YAML1, TOML1, JSON, Ensemble, Bueno, DublinCoreDatacard, SchemaOrgDatacard, GoogleDatacard, Oceans11Datacard")
            raise ValueError
        self.t.artifact_handler(interaction_type='ingest')
        self.t.active_metadata = OrderedDict()

    def query(self, statement, collection = False):
        """
        Executes a SQL query on the first activated backend.

        `statement` : str
            A SQL query to execute. Only `SELECT` and `PRAGMA` statements are allowed.

        `collection` : bool, optional, default False.
            If True, returns the result as a pandas.DataFrame
            
            If False, (default), prints the result.
        """
        df = self.t.artifact_handler(interaction_type='query', query=statement)
        if not collection:
            headers = df.columns.tolist()
            rows = df.values.tolist()
            self.t.table_print_helper(headers, rows)
            print()
        else:
            df.attrs['table_name'] = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', statement, re.IGNORECASE)[0][0]
            return df
        
    def find(self, query, collection = False):
        """
        Finds all individual datapoints matching the `query` input from the first activated backend

        `query`: int, float, or str
            The value to search for across all tables in the backend. Matching is performed
            at the individual cell level.

        `collection` : bool, optional, default False. 
            If True, returns a list of structured objects (one for each table with data matching `query`) 
            that include data/metadata for each match. Each object follows the FindObject structure:

                - table_name : str
                    Name of table where 'query' was found.
                    **DO NOT modify this if you plan to call DSI.update()**

                - column_name : str
                    The first column where 'query` was matched in this table

                - row_numbers : list of int
                    Indices (row numbers) where `query` was found in this table. 
                    **DO NOT modify this list if you plan to cal DSI.update()**

                - collection : Pandas.DataFrame
                    Subset of the table containing all rows where `query` was found
            
            If False (default), prints the matches in the table
        """
        find_data = self.t.find_cell(query, row=True)
        if collection == False:
            for val in find_data:
                print(f"Table: {val.t_name}")
                print(f"  - Columns: {val.c_name}")
                print(f"  - Row Number: {val.row_num}")
                print(f"  - Data: {val.value}")
            print()
        else:
            find_dict = {}
            for val in find_data:
                # ALL COLS WHERE QUERY IS FOUND
                # all_cols = [i for i, x in enumerate(val.value) if str(query) in str(x)]

                if val.t_name not in find_dict:
                    find_dict[val.t_name] = FindObject()
                    find_dict[val.t_name].table_name = val.t_name
                    index = next((i for i, x in enumerate(val.value) if str(query) in str(x)), -1)
                    find_dict[val.t_name].column_name = index
                    # find_dict[val.t_name].column_name = sorted(all_cols)
                    find_dict[val.t_name].row_numbers = []
                    find_dict[val.t_name].row_numbers.append(val.row_num)
                    find_dict[val.t_name].collection = pd.DataFrame([val.value], columns=val.c_name)

                if val.row_num not in find_dict[val.t_name].row_numbers:
                    find_dict[val.t_name].row_numbers.append(val.row_num)
                    find_dict[val.t_name].collection.loc[len(find_dict[val.t_name].collection)] = val.value
                    
                    # CHECK IF COLUMNS LIST NEEDS TO BE UPDATED
                    # curr_cols = find_dict[val.t_name].column_name
                    # diff = set(all_cols) - set(curr_cols)
                    # if diff:
                    #     find_dict[val.t_name].column_name = sorted(set(curr_cols).union(diff))
            
            table_list = list(find_dict.values())
            for ind, item in enumerate(table_list):
                table_list[ind].column_name = item.collection.columns[item.column_name]

                # IF COLUMN IS A LIST OF MATCHING COLS
                # table_list[ind].column_name = [item.column_name[i] for i in item.collection.columns] 
            
            return table_list

    def update(self, collection):
        """
        Updates data in one or more tables in the first activated backend using the provided input. 
        Expected to be used after manipulating outputs of `find()` or `query()`

        `collection` : List of FindObject, FindObject, or pandas.DataFrame
            The object used to update table data. Valid inputs are:

            - List of `FindObject` (from `find()` output when collection = True)
            - Single `FindObject`  (one element in `find()` output when collection = True)
            - `pandas.DataFrame` (from `query()` output when collection = True)

                - Corresponding table in the backend will be completely overwritten.
                  Ensure the data is complete and properly structured

        - NOTE: Edited table cannot delete columns from the original table, only edit or append new ones.
        - NOTE: If a DataFrame includes edits to a column that is a user-defined primary key, row order may change upon reinsertion.
        """
        if isinstance(collection, (list, FindObject)):
            if isinstance(collection, FindObject):
                collection = [collection]
            if isinstance(collection, list) and len(collection) > 0 and isinstance(collection[0], FindObject):
                pass
            else:
                raise ValueError("If input is a list, must be a list of FindObjects which was the output of find()")

            for find_obj in collection:
                actual_df = self.t.display(find_obj.table_name, num_rows=-101)
                input_df = find_obj.collection
                if not set(actual_df.columns).issubset(set(input_df.columns)):
                    errorStmt = f"The columns in {find_obj.table_name} object's dataframe, must contain all columns from the table in the backend"
                    raise ValueError(errorStmt)
                new_cols = list(set(input_df.columns) - set(actual_df.columns))
                if new_cols:
                    for col in new_cols:
                        actual_df[col] = None
                    actual_df = actual_df[input_df.columns]
                
                for col in actual_df.columns:
                    common_dtype = np.find_common_type([actual_df[col].dtype, input_df[col].dtype], [])
                    actual_df[col] = actual_df[col].astype(common_dtype)
                    input_df[col] = input_df[col].astype(common_dtype)

                # IF ROW NUMBERS ARE 1-INDEXED NOT 0-INDEXED
                id_list = [x - 1 for x in find_obj.row_numbers]
                actual_df.loc[id_list] = input_df.values

                # IF ROW NUMBERS ARE 0-INDEXED
                # actual_df.loc[find_obj.row_numbers] = input_df.values

                self.t.overwrite_table(find_obj.table_name, actual_df)
    
        elif isinstance(collection, pd.DataFrame):
            self.t.overwrite_table(collection.attrs['table_name'], collection)
        else:
            raise ValueError("collection can only be a FindObject, list of FindObjects, or Pandas DataFrame to update the backend")
    
    def nb(self):
        """
        Generates a Python notebook and stores data from the first activated backend
        """
        self.t.artifact_handler(interaction_type="notebook")
        print("Notebook .ipynb and .html generated.")

    def list_writers(self):
        """
        Prints a list of valid writers that can be used in the `writer_name` argument in `write()`
        """
        print("\nValid Readers for `reader_name` in read():", self.t.VALID_WRITERS, "\n")
        print("ER_Diagram  : Creates a visual ER diagram image based on all tables in DSI.")
        print("Table_Plot  : Generates a plot of numerical data from a specified table.")
        print("Csv_Writer  : Exports the data of a specified table to a CSV file.")
        print()

    def write(self, filename, writer_name, table_name = None):
        """
        Exports data from a DSI backend using the specified `writer_name`.

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
        if not self.t.loaded_backends:
            raise ValueError("Must load a backend first. Call backend() before this")
        
        self.t.artifact_handler(interaction_type='process')
        if writer_name == "ER_Diagram":
            self.t.load_module('plugin', 'ER_Diagram', 'writer', filename=filename)
        elif writer_name == "Table_Plot":
            self.t.load_module('plugin', 'Table_Plot', 'writer', filename=filename, table_name = table_name)
        elif writer_name == "Csv_Writer":
            self.t.load_module('plugin', 'Csv_Writer', 'writer', filename=filename, table_name = table_name)
            writer_name = "Csv"
        else:
            print("Please check your spelling of the 'writer_name' argument as it does not exist in DSI")
            print(f"Eligible writers are: {self.list_writers()}")
            return
        self.t.transload()
        self.t.active_metadata = OrderedDict()
        print(f"{writer_name} written to {filename} complete.")
    
    def list(self):
        """
        Prints the names and dimensions (rows x columns) of all tables in the first active backend
        """
        self.t.list()

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
        self.t.summary(table_name, num_rows)
    
    def num_tables(self):
        """
        Prints the number of tables in the first activated backend
        """
        self.t.num_tables()

    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data from a specified table in the first activated backend.
        
        `table_name` : str
            Name of the table to display.
         
        `num_rows` : int, optional, default=25
            Maximum number of rows to print. If the table contains fewer rows, only those are shown.

        `display_cols` : list of str, optional
            List of specific column names to display from the table. 

            If None (default), all columns are displayed.
        """
        self.t.display(table_name, num_rows, display_cols)

    def close(self):
        """
        Closes the connection and finalizes the changes to the backend
        """
        self.t.close()
    
    #help, edge-finding (find this/that)
    def get(self, dbname):
        pass
    def move(self, filepath):
        pass
    def fetch(self, fname):
        pass
