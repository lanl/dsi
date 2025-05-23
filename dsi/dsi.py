from dsi.core import Terminal, Sync

class DSI():
    '''
    A user-facing abstration for DSI's Core middleware interface.

    The DSI Class abstracts Core.Terminal for managing metadata and Core.Sync for data management and movement.
    '''

    def __init__(self):
        self.t = Terminal(debug = 0, runTable=False)
        self.s = Sync()

    def list_readers(self):
        """
        Prints a list of valid readers that can be specified in the 'reader_name' argument in read()
        """
        print("Valid Readers: Csv, YAML1, TOML1, JSON, Schema, Ensemble, Bueno, DublinCoreDatacard, SchemaOrgDatacard, GoogleDatacard, Oceans11Datacard \n")
        print("Oceans11Datacard loads in metadata describing a dataset to be stored in the oceans11 DSI data server (oceans11.lanl.gov). Input format is YAML")
        print("DublinCoreDatacard loads in a dataset's metadata which conforms to Dublin Core. Input format is XML")
        print("SchemaOrgDatacard loads in a dataset's metadata which conforms to schema.org. Input format is JSON")
        print("Schema loads in a complex JSON schema that describes tables relations for a structured relational database like Sqlite and DuckDB.")
        print("Bueno captures performance data from Bueno (github.com/lanl/bueno). Input format is a dictionary in a text file ending in .data")
        print("Csv loads in data from CSV files that can only be for one table in each separate call")
        print("YAML1 loads in data from YAML files of a particular structure")
        print("TOML1 loads in data from TOML files of a particular structure")
        print("Ensemble loads in data from a CSV file and generates a simulation table alongside it. Each row of data should be a separate simulation run")
        print("JSON loads in data from JSON files that can only be for one table in each separate call")
        print()

    def read(self, filenames, reader_name, table_name = None):
        """
        Runs a reader to load data into DSI.

        `filenames`: name(s) of the data file(s) to load into DSI
            
            - if reader_name = "Csv" ---> file extension should be .csv
            - if reader_name = "YAML1" ---> file extension should be .yaml or .yml
            - if reader_name = "TOML1" ---> file extension should be .toml
            - if reader_name = "JSON" ---> file extension should be .json
            - if reader_name = "Schema" ---> file extension should be .json
            - if reader_name = "Ensemble" ---> file extension should be .csv
            - if reader_name = "Bueno" ---> file extension should be .data
            - if reader_name = "DublinCoreDatacard" ---> file extension should be .xml
            - if reader_name = "SchemaOrgDatacard" ---> file extension should be .json
            - if reader_name = "GoogleDatacard" ---> file extension should be .yaml or .yml
            - if reader_name = "Oceans11Datacard" ---> file extension should be .yaml or .yml

        `reader_name`: name of the DSI reader to use. Call list_readers() to see a list of valid readers

        `table_name`: optional, default None. If `filenames` only stores one table of data, users can specify name for that table

            - Csv, JSON, and Ensemble readers are only ones to accept this input
        
        """
        if reader_name.lower() == "oceans11datacard":
            self.t.load_module('plugin', 'Oceans11Datacard', 'reader', filenames=filenames)
        elif reader_name.lower() == "dublincoredatacard":
            self.t.load_module('plugin', 'DublinCoreDatacard', 'reader', filenames=filenames)
        elif reader_name.lower() == "schemaorgdatacard":
            self.t.load_module('plugin', 'SchemaOrgDatacard', 'reader', filenames=filenames)
        elif reader_name.lower() == "googledatacard":
            self.t.load_module('plugin', 'GoogleDatacard', 'reader', filenames=filenames)
        elif reader_name.lower() == "schema":
            if isinstance(filenames, list):
                raise ValueError("Cannot ingest more than one schema at a time")
            self.t.load_module('plugin', 'Schema', 'reader', filename=filenames)
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
            print("Eligible readers are: Csv, YAML1, TOML1, JSON, Schema, Ensemble, Bueno, DublinCoreDatacard, SchemaOrgDatacard, GoogleDatacard, Oceans11Datacard")
            print()

    def list_backends(self):
        """
        Prints a list of valid backends that can be specified in the 'backend_name' argument in backend()
        """
        print("Sqlite, DuckDB\n")
        print("Sqlite: Python based SQL database and backend; the default DSI API backend.")
        print("DuckDB: In-process SQL database designed for fast queries on large data files.")
        print()

    def backend(self, filename, backend_name = "Sqlite"):
        """
        Activates a backend, default is Sqlite unless specified. 
        Uses can now call the ingest(), query(), or process() functions.

        `filename`: name of the backend file
            - if backend_name = "Sqlite" ---> file extension can be .db, .sqlite, .sqlite3
            - if backend_name = "DuckDB" ---> file extension can be .duckdb, .db
            
        `backend_name`: either 'Sqlite' or 'DuckDB. Default is Sqlite
        """
        if backend_name.lower() == 'sqlite':
            self.t.load_module('backend','Sqlite','back-write', filename=filename)
        elif backend_name.lower() == 'duckdb':
            self.t.load_module('backend','DuckDB','back-write', filename=filename)
        else:
            print("Please check the 'backend_name' argument as that is not supported by DSI now")
            print(f"Eligible backend_names are: Sqlite, DuckDB")

    def ingest(self):
        """
        Ingests data from all previously called read() functions into active backends from backend().
        """
        self.t.artifact_handler(interaction_type='ingest')
        print("Ingest complete.")

    def query(self, statement):
        """
        Queries data from first activated backend based on specified `statement`. Prints data as a dataframe

        `statement`: query to run on a backend. `statement` can only be a SELECT or PRAGMA query.
        """
        df = self.t.artifact_handler(interaction_type='query', query=statement)
        headers = df.columns.tolist()
        rows = df.values.tolist()
        self.t.table_print_helper(headers, rows)
    
    def process(self):
        """
        Reads data from first activated backend into DSI memory. 
        """
        self.t.artifact_handler(interaction_type='process')
    
    def nb(self):
        """
        Generates a Python notebook and stores data from the first activated backend
        """
        self.t.artifact_handler(interaction_type="notebook")
        print("Notebook .ipynb and .html generated.")

    def list_writers(self):
        """
        Prints a list of valid writers that can be specified in the 'writer_name' argument in write()
        """
        print(self.t.VALID_WRITERS)
        print("ER_Diagram creates an image of an ER Diagram based on data stored in DSI.")
        print("Table_Plot generates a plot of a specified table's numerical data that is stored in DSI.")
        print("Csv_Writer creates a CSV of a specified table whose data is stored in DSI.")
        print()

    def write(self, filename, writer_name, table_name = None):
        """
        Runs a writer to export data from DSI.
        If data to export is in a backend, first call process() before write().

        `filename`: output file name

            - if writer_name = "ER_Diagram" ---> file extension can be .png, .pdf, .jpg, .jpeg
            - if writer_name = "Table_Plot" ---> file extension can be .png, .jpg, .jpeg
            - if writer_name = "Csv_Writer" ---> file extension can only be .csv

        `writer_name`: name of the DSI write to use. Call list_writers() to see a list of valid readers

        `table_name`: optional if writer_name = "ER_Diagram". Required for Table_Plot and Csv_Writer to export correct table
        """
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
        print(f"{writer_name} written to {filename} complete.")
    
    def findt(self, query):
        """
        Finds all tables that match `query` input in the first loaded backend
        """
        data = self.t.find_table(query)
        for val in data:
            print(f"Table: {val.t_name}")
            print(f"  - Columns: {val.c_name}")
            print(f"  - Search Type: {val.type}")
            print(f"  - Value: \n{val.value}")
        print()

    def findc(self, query, range = False):
        """
        Finds all columns that match `query` input in the first loaded backend.

        `range`: Default is False. If False, then the printed `value` is data of each matching column.
        If True, then the printed `value` is the min/max of each matching column
        """
        data = self.t.find_column(query, range)
        for val in data:
            print(f"Table: {val.t_name}")
            print(f"  - Column: {val.c_name}")
            print(f"  - Search Type: {val.type}")
            print(f"  - Value: {val.value}")
        print()

    def find(self, query, row = False):
        """
        Finds all individual datapoints that match `query` input in the first loaded backend

        `row`: Default is False. If False, then printed `value` is the actual cell that matches `query`.
        If True, then printed `value` is whole row of data where a cell matches `query`
        """
        data = self.t.find_cell(query, row)
        for val in data:
            print(f"Table: {val.t_name}")
            print(f"  - Column(s): {val.c_name}")
            print(f"  - Search Type: {val.type}")
            print(f"  - Row Number: {val.row_num}")
            print(f"  - Value: {val.value}")
        print()

    def list(self):
        """
        Prints a list of all tables and their dimensions in the first loaded backend
        """
        self.t.list() # terminal function already prints

    def summary(self, table_name = None, num_rows = 0):
        """
        Prints data and numerical metadata of tables from the first loaded backend. Output varies depending on parameters

        `table_name`: default is None. When specified only that table's numerical metadata is printed. 
        Otherwise every table's numerical metdata is printed

        `num_rows`: default is 0. When specified, data from the first N rows of a table are printed. 
        Otherwise, only the total number of rows of a table are printed. 
        The tables whose data is printed depends on the `table_name` parameter.
        """
        self.t.summary(table_name, num_rows) # terminal function already prints
    
    def num_tables(self):
        """
        Prints number of tables in the first loaded backend
        """
        self.t.num_tables() # terminal function already prints

    def display(self, table_name, num_rows = 25, display_cols = None):
        """
        Prints data of a specified table from the first loaded backend.
        
        `table_name`: table whose data is printed
         
        `num_rows`: Optional numerical parameter limiting how many rows are printed. Default is 25.

        `display_cols`: Optional parameter specifying which columns in `table_name` to display. Must be a Python list object
        """
        self.t.display(table_name, num_rows, display_cols) # terminal function already prints

    def close(self):
        """
        Closes the connection and finalizes the changes to the backend
        """
        self.t.close()
    
    #help, query?, edge-finding (find this/that)
    def get(self, dbname):
        pass
    def move(self, filepath):
        pass
    def fetch(self, fname):
        pass