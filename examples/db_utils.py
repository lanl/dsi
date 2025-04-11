import sqlite3
import logging
import time
import csv
import shutil
import os
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime




logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='artimis.log',  # Log to a file called 'app.log'
    filemode='a'            # Append to the log file (use 'w' to overwrite)
)


class Store:
    '''
    Create a storage for data, eventually will be replaced by DSI

    Attributes:
        db_name (str): name of the database
    '''

    def __init__(self):
        '''
        Initializes the storage

        Args:
            name (str): name of the db to create
        '''

        # Create a logger for this class
        self.logger = logging.getLogger(self.__class__.__name__)

        now = datetime.now()
        date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"-------------------------------------")
        self.logger.info(f"Start time: {date_time_str}")


    def close(self):
        self.conn.close()


    def is_sqlite3_file(self, filename):
        if not os.path.isfile(filename):
            return False
        with open(filename, 'rb') as f:
            header = f.read(16)
        return header == b'SQLite format 3\x00'


    def copy_sqlite_db(self, dest_path):
        shutil.copyfile(self.path , dest_path)
        


    def connect_to_db(self, path):
        '''
        Create or connect to a database with the path specified

        Args:
            path (str) : path to where to create or db to connect to
        '''
        self.path = path
        self.conn = sqlite3.connect(path)

        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

        # Fetch and print all table names
        
        tables = cursor.fetchall()
        #print(f"{len(tables)} tables were found.")
        self.logger.info(f"List of tables: '{tables}'")
        self.logger.info(f"Database '{path}' connected to.")


    def sql_to_db(self, sql_file_path, db_path):
        if not os.path.isfile(sql_file_path):
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        with open(sql_file_path, 'r') as f:
            sql_script = f.read()

        self.path = path
        self.conn = sqlite3.connect(path)
        cursor = self.conn.cursor()

        try:
            cursor.executescript(sql_script)
            self.conn.commit()
            print("SQL script executed successfully.")
        except sqlite3.Error as e:
            print("An error occurred while executing SQL:")
            print(e)


    def get_num_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        
        tables = cursor.fetchall()
        return len(tables)



    def import_csv(self, csv_file_path, table_name):
        cursor = self.conn.cursor()

        with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # First line as column names

            # Create table SQL statement (all fields as TEXT for simplicity)
            columns = ', '.join([f'"{col}" TEXT' for col in headers])
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
            cursor.execute(create_table_sql)

            # Prepare the insert statement
            placeholders = ', '.join(['?'] * len(headers))
            insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

            # Insert all rows
            for row in reader:
                cursor.execute(insert_sql, row)

        self.conn.commit()




    def infer_column_type(self, values):
        """Infer SQLite-compatible column type from a list of sample values."""
        for v in values:
            if v == '' or v is None:
                continue
            try:
                int(v)
            except ValueError:
                try:
                    float(v)
                except ValueError:
                    return "TEXT"
                return "REAL"
        return "INTEGER"



    def load_csv_to_sqlite(self, csv_path, table_name=None, sample_size=100):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file '{csv_path}' not found.")

        # Derive table name from file if not provided
        if table_name is None:
            table_name = os.path.splitext(os.path.basename(csv_path))[0]

        cursor = self.conn.cursor()

        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            data_sample = [row for _, row in zip(range(sample_size), reader)]

        # Transpose to infer type per column
        columns_data = list(zip(*data_sample))
        inferred_types = [self.infer_column_type(col) for col in columns_data]

        # Create table
        columns = ', '.join(f'"{name}" {col_type}' for name, col_type in zip(headers, inferred_types))
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')  # Remove if exists for clean load
        cursor.execute(f'CREATE TABLE "{table_name}" ({columns})')

        # Re-read file to insert all rows
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header again
            placeholders = ', '.join(['?'] * len(headers))
            insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            cursor.executemany(insert_sql, reader)

        self.conn.commit()
        print(f"Loaded '{csv_path}' into table '{table_name}.")


    def load_pq_to_sqlite(self, pq_path, table_name=None):
        # Determine table name from file if not provided
        if table_name is None:
            table_name = os.path.splitext(os.path.basename(pq_path))[0]

        # Read Parquet file using pyarrow
        table = pq.read_table(pq_path)

        # Infer SQLite-compatible types from PyArrow schema
        def infer_sqlite_type(pa_type):
            if pa.types.is_integer(pa_type):
                return "INTEGER"
            elif pa.types.is_floating(pa_type):
                return "REAL"
            elif pa.types.is_boolean(pa_type):
                return "BOOLEAN"
            elif pa.types.is_timestamp(pa_type):
                return "TIMESTAMP"
            else:
                return "TEXT"

        schema = table.schema
        column_names = schema.names
        column_types = [infer_sqlite_type(field.type) for field in schema]

        # Create SQL table schema
        columns_def = ", ".join(
            f'"{name}" {dtype}' for name, dtype in zip(column_names, column_types)
        )
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def});'

        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)

        # Prepare and execute insert statements
        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

        # Convert PyArrow Table to batches and rows
        for batch in table.to_batches():
            records = batch.to_pylist()
            values = [tuple(record[col] for col in column_names) for record in records]
            cursor.executemany(insert_sql, values)

        self.conn.commit()
        print(f"Loaded '{pq_path}' into table '{table_name}.")


    def pretty_print(self, headers, rows, max_rows=25):
        #Determine max width for each column
        col_widths = [max(len(str(h)), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]

        # Print header
        header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
        print("\n" + header_row)
        print("-" * len(header_row))

        # Print each row
        count = 0
        for row in rows:
            print(" | ".join(f"{row[i]:<{col_widths[i]}}" for i in range(len(row))))

            count = count + 1
            if count == max_rows:
                print(f"  ... showing 25 of {len(rows)}")
                break


        
        print("\n")


    def get_schema(self, table_name):
        cursor = self.conn.cursor()

        # Get column metadata
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        # Extract and prepare data for printing
        #headers = ["Column", "Type", "Not Null", "PK", "Default"]
        headers = ["Column", "Type", "Not Null", "PK"]
        rows = []
        for _, name, dtype, notnull, default, pk in columns:
            rows.append([str(name), str(dtype), str(bool(notnull)), str(bool(pk))])#, str(default)])

        return headers, rows
    


    def summarize_table(self, table_name: str):
        """
        Compute column statistics from a SQLite table and return results as headers and row data.

        Parameters
        ----------
        db_path : str
            Path to the SQLite database file.
        table_name : str
            Name of the table to analyze.

        Returns
        -------
        tuple
            A tuple containing:
                - headers: list of column headers
                - rows: list of rows, each as a list of values
        """
        cursor = self.conn.cursor()

        # Get column metadata
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()

        numeric_types = {'INTEGER', 'REAL', 'FLOAT', 'NUMERIC', 'DECIMAL', 'DOUBLE'}

        headers = ['column', 'type', 'min', 'max', 'avg', 'std_dev']
        rows = []

        for col in columns_info:
            col_name = col[1]
            col_type = col[2].upper()
            is_primary = col[5] > 0
            display_name = f"{col_name}*" if is_primary else col_name

            if any(nt in col_type for nt in numeric_types):
                cursor.execute(f"""
                    WITH stats AS (
                        SELECT 
                            AVG("{col_name}") AS mean
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
                """)
                min_val, max_val, avg_val, std_dev = cursor.fetchone()
            else:
                min_val = max_val = avg_val = std_dev = None

            rows.append([display_name, col_type, min_val, max_val, avg_val, std_dev])

        return headers, rows



    def get_tables_info(self):
        cursor = self.conn.cursor()

        # Step 1: Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]

        db_info = []
        for table in tables:
            table_info = []
            table_info.append(table)

            # Get column names (headers)
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            table_info.append(len(columns))

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            table_info.append(row_count)

            db_info.append(table_info)

        return db_info



    def insert_col_data(self, table_name, columns, data):
        '''
        Insert data into a table with column names provided

        Args:
            table_name (str) : name of the table to use
            columns (list) : headers in a list
            data (list) : data in a list of lists
        '''
        columns_str = ', '.join(columns)  
        placeholders = ', '.join('?' for _ in data)

        cursor = self.conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});", data)
        self.conn.commit()
        cursor.close()



    def insert_bulk(self, table_name, column_names, data_list):
        '''
        Inserts multiple rows into an SQLite table efficiently using executemany().
        
        :param table_name: Name of the target table
        :param column_names: List of column names to insert data into
        :param data_list: List of tuples containing the values for each row
        :param db_path: Path to the SQLite database file
        '''
        if not data_list:
            return  # Avoid inserting an empty list

        try:
            cursor = self.conn.cursor()

            # Generate the SQL query dynamically based on the number of columns
            columns_str = ", ".join(column_names)
            placeholders = ", ".join(["?" for _ in column_names])  # Create placeholders for values
            sql_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Execute batch insert
            cursor.executemany(sql_query, data_list)

            # Commit transaction for efficiency
            self.conn.commit()
            print(f"Inserted {len(data_list)} rows into {table_name}")

        except sqlite3.Error as e:
            print(f"SQLite Error: {e}")



    def insert_data(self, table_name, data):
        '''
        Insert data into a table with just the data, risky!!!

        Args:
            table_name (str) : name of the table to use
            data (list) : data in a list of lists
        '''
        placeholders = ', '.join('?' for _ in data)
        
        cursor = self.conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", data)
        self.conn.commit()
        cursor.close()



    def add_colums(self, table_name, column_names, column_types):
        '''
        Add columns to a table

        Args:
            table_name (str) : name of the table to use
            column_names (list) : data in a list of lists
            column_types (list) : types in a list of lists
        '''

        cursor = self.conn.cursor()

        # Iterate through each column to add it to the table
        for index, column_name in enumerate(column_names):
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_types[index]}")
                print(f"Column '{column_name}' added successfully.")
            except sqlite3.OperationalError as e:
                print(f"Error adding column '{column_name}': {e}")

        self.conn.commit()
        cursor.close()




    def check_if_col_exists(self, table_name, column_name):
        '''
        Check if column exists

        Args:
            table_name (str) : name of table
            column_name (str): name of column

        Returns:
            boolean : true if column exists
        '''

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")

        columns = cursor.fetchall()
        column_exists = any(col[1] == column_name for col in columns)

        cursor.close()

        if column_exists:
            return True
        else:
            return False
        

    def output_csv(self, column_names, rows, csv_path):
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)  # write header
            writer.writerows(rows)         # write data

        print(f"Query results exported to '{csv_path}'.\n")




    def query(self, query):
        '''
        Get data out from a table

        Args:
            query (str) : query to run on the 

        Returns:
            list of tuples : all the data from the table
        '''

        start_time = time.time()
        cursor = self.conn.cursor()

        try:
            # Try executing the query
            cursor.execute(query)
            results = cursor.fetchall()
            col_names = [description[0] for description in cursor.description]
        except sqlite3.Error as e:
            print(f"Invalid SQL query: {e}")
            results = None
        

        stop_time = time.time()
        self.logger.info(f"query: Retreiving data for query took '{stop_time - start_time}' s.")

        cursor.close()

        return results, col_names
    


    def exec_script(self, script):
        '''
        Execute a sql script

        Args:
            script (str) : script to run
        '''

        start_time = time.time()
        cursor = self.conn.cursor()

        try:
            # Try executing the query
            cursor.executescript(script)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Invalid SQL query: {e}")
        

        stop_time = time.time()
        self.logger.info(f"query: Retreiving data for query took '{stop_time - start_time}' s.")

        cursor.close()



