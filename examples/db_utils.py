import sqlite3
import logging
import time
import csv
import shutil
import os
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


    def copy_sqlite_db(self, dest_path):
        shutil.copyfile(self.path , dest_path)
        print(f"Database saved to '{dest_path}'.")


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



    def get_num_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        
        tables = cursor.fetchall()
        print(f"Database has {len(tables)} table/s.")



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



    def get_schema(self, table_name):
        cursor = self.conn.cursor()

        print(f"\n=== Schema for table '{table_name}' ===")

        # Get column metadata
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        # Extract and prepare data for printing
        headers = ["Column", "Type", "Not Null", "PK", "Default"]
        rows = []
        for _, name, dtype, notnull, default, pk in columns:
            rows.append([str(name), str(dtype), str(bool(notnull)), str(bool(pk)), str(default)])

        # Determine max width for each column
        col_widths = [max(len(str(h)), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]

        # Print header
        header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
        print("\n" + header_row)
        print("-" * len(header_row))

        # Print each row
        for row in rows:
            print(" | ".join(f"{row[i]:<{col_widths[i]}}" for i in range(len(row))))



    def info(self):
        cursor = self.conn.cursor()

        # Step 1: Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            print(f"Table: {table}")

            # Get column names (headers)
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            print(f"  Columns: {columns}")

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            print(f"  Row count: {row_count}\n")



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
        

    def pretty_query(self, query):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [description[0] for description in cursor.description]

            # Determine column widths
            col_widths = [max(len(str(value)) for value in [col] + [row[i] for row in rows]) for i, col in enumerate(col_names)]

            # Print header
            header = " | ".join(f"{col:<{col_widths[i]}}" for i, col in enumerate(col_names))
            print(header)
            print("-" * len(header))

            # Print each row
            for row in rows:
                print(" | ".join(f"{str(value):<{col_widths[i]}}" for i, value in enumerate(row)))

        except sqlite3.Error as e:
            print(f"Query error: {e}")


    def query_to_csv(self, query, csv_path):
        cursor = self.conn.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(column_names)  # write header
                writer.writerows(rows)         # write data

            print(f"Query results exported to '{csv_path}'.")

        except sqlite3.Error as e:
            print(f"Query failed: {e}")



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
        except sqlite3.Error as e:
            print(f"Invalid SQL query: {e}")
            results = None
        

        stop_time = time.time()
        self.logger.info(f"query: Retreiving data for query took '{stop_time - start_time}' s.")

        cursor.close()

        return results
    


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



