from dsi.core import Terminal, Sync
from collections import OrderedDict
import numpy as np
import pandas as pd
import os
import sys
from contextlib import redirect_stdout

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
        print(f"Successfully loaded the schema file, {filename}")

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
                - "Cloverleaf"           → /path/to/data/dir/
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
        if isinstance(filenames, str) and not os.path.exists(filenames):
            sys.exit("read() ERROR: The input file must be a valid filepath. Please check again.")
        if isinstance(filenames, list) and not all(os.path.exists(f) for f in filenames):
            sys.exit("read() ERROR: All input files must have a valid filepath. Please check again.")
        
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
            elg = "CSV, YAML1, TOML1, JSON, Ensemble, Cloverleaf, Bueno, DublinCoreDatacard, SchemaOrgDatacard, GoogleDatacard, Oceans11Datacard"
            sys.exit(f"Eligible readers are: {elg}")

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
        Executes a SQL query on the first activated backend.

        `statement` : str
            A SQL query to execute. Only `SELECT` and `PRAGMA` statements are allowed.

        `collection` : bool, optional, default False.
            If True, returns the result as a pandas.DataFrame
            
            If False (default), prints the result.

        `return`: If the `statement` is incorrectly formatted, then nothing is returned or printed
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot query() until all associated data is loaded after a complex schema")
        
        try:
            df = self.t.artifact_handler(interaction_type='query', query=statement)
        except Exception as e:
            sys.exit(f"query() ERROR: {e}")
        if not collection:
            print(f"Printing the result of the SQL query: {statement}")
            headers = df.columns.tolist()
            rows = df.values.tolist()
            self.t.table_print_helper(headers, rows)
            print()
        else:
            print(f"Storing the result of the SQL query: {statement} as a collection")
            df.attrs['table_name'] = self.t.get_table_names(statement)[0]
            df.insert(0, "dsi_table_name", df.attrs['table_name'])
            return df
    
    def get_table(self, table_name, collection = False):
        """
        Retrieves all data from a specified table without requiring knowledge of a particular backend's query language.
        
        This method offers a simplified alternative to `query()` for retrieving a full table data without using SQL.

        `table_name` : str
            Name of the table from which all data will be retrieved.

        `collection` : bool, optional, default False.
            If True, returns the result as a pandas.DataFrame
            
            If False (default), prints the result.
        
        `return`: If `table_name` does not exist in the backend, then nothing is returned or printed
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot get a table of data until all associated data is loaded after a complex schema")
        
        try:
            df = self.t.get_table(table_name)
        except Exception as e:
            sys.exit(f"get_table() ERROR: {e}")
        if not collection:
            print(f"Printing all data from the table: {table_name}")
            headers = df.columns.tolist()
            rows = df.values.tolist()
            self.t.table_print_helper(headers, rows)
            print()
        else:
            print(f"Storing all data for the table: {table_name} as a collection")
            df.attrs['table_name'] = table_name
            df.insert(0, "dsi_table_name", df.attrs['table_name'])
            return df
        
    def find(self, query, collection = False):
        """
        Finds all individual datapoints matching the `query` input from the first activated backend

        `query` : int, float, or str
            The value to search for at the cell level, across all tables in the backend.

        `collection` : bool, optional, default False. 
            If True, returns a list of pandas DataFrames, each representing a subset of rows from a table where `query` was found.
            
            If False (default), prints the matching rows directly.

        `return` : If there are no matches found, then nothing is returned or printed
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot find() until all associated data is loaded after a complex schema")
        if isinstance(query, str): 
            print(f"Finding all instances of '{query}' in the active DSI backend")
        else:
            print(f"Finding all instances of {query} in the active DSI backend")

        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                find_data = self.t.find_cell(query, row=True)
        except Exception as e:
            sys.exit(f"find() ERROR: {e}")
        if not isinstance(find_data, list):
            if isinstance(query, str):
                print(f"'{query}' was not found in this backend")
            else:
                print(f"{query} was not found in this backend")
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
            find_dict = {}
            for val in find_data:
                if val.t_name not in find_dict:
                    find_dict[val.t_name] = pd.DataFrame([val.value], columns=val.c_name)
                    find_dict[val.t_name].attrs['table_name'] = val.t_name
                    find_dict[val.t_name].attrs['row_num'] = [val.row_num]

                if val.row_num not in find_dict[val.t_name].attrs['row_num']:
                    find_dict[val.t_name].loc[len(find_dict[val.t_name])] = val.value
                    find_dict[val.t_name].attrs['row_num'].append(val.row_num)

            table_list = list(find_dict.values())
            for table in table_list:
                table.insert(0, "dsi_table_name", table.attrs['table_name'])
            
            return table_list

    def update(self, collection):
        """
        Updates data in one or more tables in the first activated backend using the provided input. 
        Intended to be used after modifying the output of `find()`, `query()`, or `get_table()`

        `collection` : pandas.DataFrame or list of pandas.DataFrame
            The data used to update the backend.
            Must be a direct or modified output of `find()`, `query()` or `get_table()` to ensure required metadata is preserved.

            - `find()` returns one or more DataFrames depending on the number of matches.
            - `query()` and `get_table()` return a single DataFrame corresponding to one table.
              
                - If this DataFrame is the input, the corresponding table in the backend will be completely overwritten by the input.

        - NOTE: Some Pandas operations can delete the hidden metadata which will raise errors when trying to update.
        - NOTE: Columns from the original table cannot be deleted during update. Only edits or column additions are allowed.
        - NOTE: If a updates affect a user-defined primary key column, row order may change upon reinsertion.
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot update() until all associated data is loaded after a complex schema")
        print("Updating the active DSI backend with the input collection of data")

        if isinstance(collection, pd.DataFrame) and 'row_num' in collection.attrs:
            collection = [collection]
        if isinstance(collection, list):
            if not all(isinstance(item, pd.DataFrame) for item in collection):
                sys.exit("update() ERROR: If input is a list, must be a list of Pandas DataFrames")
            if not all('row_num' in item.attrs for item in collection):
                sys.exit("update() ERROR: Hidden metadata was deleted from input DataFrame list. Please check user operations again")

            table_name_list = []
            dataframe_list = []
            for table_df in collection:
                table_name = table_df.attrs["table_name"]
                row_num_list = table_df.attrs["row_num"]

                try:
                    actual_df = self.t.display(table_name, num_rows=-101)
                except Exception as e: # skip update if this table doesn't exist and is a find table
                    if 'dsi_table_name' in table_df.columns:
                        print_msg = table_df['dsi_table_name'][0]
                    else:
                        print_msg = f"the table with columns {table_df.columns.tolist()}"
                    print()
                    print(f"WARNING: Cannot update {print_msg} as it does not exist in the active backend.")
                    print()
                    continue
                    

                if 'dsi_table_name' in table_df.columns:
                    table_df = table_df.drop(columns='dsi_table_name')

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

                table_name_list.append(table_name)
                dataframe_list.append(actual_df)

            try:
                if len(table_name_list) > 1:
                    self.t.overwrite_table(table_name_list, dataframe_list)
                elif len(table_name_list) == 1:
                    self.t.overwrite_table(table_name_list[0], dataframe_list[0])
            except Exception as e:
                sys.exit(f"update() ERROR: {e}")
    
        elif isinstance(collection, pd.DataFrame):
            try:
                table_name = collection.attrs['table_name']
            except Exception as e:
                if 'dsi_table_name' in collection.columns:
                    table_name = collection['dsi_table_name'][0]
            if 'dsi_table_name' in collection.columns:
                collection = collection.drop(columns='dsi_table_name')
            try:
                self.t.overwrite_table(table_name, collection)
            except Exception as e:
                sys.exit(f"update() ERROR: {e}")
        else:
            sys.exit("ERROR: update() expects a DataFrame or list of DataFrames from find(), query(), or get_table()")
    
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
    
    def list(self):
        """
        Prints the names and dimensions (rows x columns) of all tables in the first active backend
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call list() until all associated data is loaded after a complex schema")
        try:
            self.t.list()
        except Exception as e:
            sys.exit(f"list() ERROR: {e}")
        

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
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call summary() until all associated data is loaded after a complex schema")
        try:
            self.t.summary(table_name, num_rows)
        except Exception as e:
            sys.exit(f"display() ERROR: {e}")
    
    def num_tables(self):
        """
        Prints the number of tables in the first activated backend
        """
        if self.schema_read == True:
            sys.exit("ERROR: Cannot call num_tables() until all associated data is loaded after a complex schema")
        try:
            self.t.num_tables()
        except Exception as e:
            sys.exit(f"num_tables() ERROR: {e}")

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
        Closes the connection and finalizes the changes to the backend
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
