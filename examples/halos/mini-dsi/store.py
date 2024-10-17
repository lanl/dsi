import sqlite3
import json
import csv
import os
import logging
import time
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='dsi.log',  # Log to a file called 'app.log'
    filemode='a'         # Append to the log file (use 'w' to overwrite)
)



class Store:
    """
    Create a storage for data

    Attributes:
        db_name (str): name of the database
    """

    def __init__(self, name):
        """Initializes the storage

        Args:
            name (str): name of the db to create
        """

        # Create a logger for this class
        self.logger = logging.getLogger(self.__class__.__name__)
        self.name = name

        now = datetime.now()
        date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"-------------------------------------")
        self.logger.info(f"Start time: {date_time_str}")


        # Create the database
        self.db_name = name + '.db'
        conn = sqlite3.connect(self.db_name)
        conn.close()


        self.logger.info(f"Database '{self.db_name}' created successfully.")



    def ingest_jsonfile(self, json_file, table_name):
        """
        Create a table from a JSON file and save to the database

        Args:
            json_file (str): name of the JSON file to load
            table_name (str): name of the table to create for that CSV file
        """

        if not os.path.exists(json_file):
            print(f"File '{json_file}' does not exist")
            return
        

        start_time = time.time()
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        #Load JSON data from a file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Dynamically create the table based on JSON keys
        # Assuming JSON is a list of dictionaries, where each dictionary is a row
        first_record = json_data[0]

        # Extract column names from the JSON keys
        columns = ', '.join([f"{key} TEXT" for key in first_record.keys()])

        # Create table with columns
        table_name = 'json_table'
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(create_table_query)

        # Step 4: Insert the JSON data into the SQLite table
        # Generate placeholder based on the number of columns
        placeholders = ', '.join(['?' for _ in first_record])

        # Insert query
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"


        # Insert each row of JSON data
        for record in json_data:
            values = tuple(record.values())
            cursor.execute(insert_query, values)


        conn.commit()
        conn.close()

        stop_time = time.time()

        print(f"Number of rows inserted: '{num_rows}'.")

        self.logger.info(f"ingest_json: Number of rows inserted: '{num_rows}'.")
        self.logger.info(f"Table {table_name} created in '{stop_time - start_time}' s.")



    def ingest_csvfile(self, csv_file, table_name):
        """
        Create a table from a CSV file and save to the database

        Args:
            csv_file (str): name of the CSV file to load
            table_name (str): name of the table to create for that CSV file
        """

        if not os.path.exists(csv_file):
            print(f"File '{csv_file}' does not exist")
            return
        

        start_time = time.time()
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Read the CSV file and dynamically create the table
        with open(csv_file, newline='') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Get the headers for the table columns
        

            # Check if the table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if cursor.fetchone() is None:
                # If the table doesn't exist, create it
                cursor.execute(f"CREATE TABLE {table_name} ({', '.join(headers)});")
                print(f"Table '{table_name}' created.")
            else:
                print(f"Table '{table_name}' already exists. Appending data...")

            # Insert CSV data into the table
            num_rows = 0
            for row in reader:
                placeholders = ', '.join(['?' for _ in row])
                cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", row)
                num_rows = num_rows + 1

        conn.commit()
        conn.close()

        stop_time = time.time()

        print(f"Number of rows inserted: '{num_rows}'.")

        self.logger.info(f"ingest_csv: Number of rows inserted: '{num_rows}'.")
        self.logger.info(f"Table {table_name} created in '{stop_time - start_time}' s.")



    def ingest_table(self, my_list, table_name, append=False):
        """
        Create/Append to a table from a List of lists and save to the database

        Args:
            my_list (list of lists): first row is header, and the rest is data
            table_name (str): name of the table to create for that CSV file
            append (bool): append data to an existing table
        """

        start_time = time.time()
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()


        if append == True:
            for row in my_list:
                placeholders = ', '.join('?' for _ in row)
                cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", row)

                conn.commit()
                conn.close()

        else:
            keys_list = my_list[0]

            # Check if the table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if cursor.fetchone() is None:
                # If the table doesn't exist, create it
                cursor.execute(f"CREATE TABLE {table_name} ({', '.join(keys_list)});")
                print(f"Table '{table_name}' created.")
            else:
                print(f"Table '{table_name}' already exists. Appending data...")


            for index, row in enumerate(my_list):
                if index == 0:
                    continue

                placeholders = ', '.join('?' for _ in row)
                cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", row)


            conn.commit()
            conn.close()

        stop_time = time.time()

        print(f"Number of rows inserted: '{len(my_list)}'.")

        self.logger.info(f"ingest_table: Number of rows inserted: '{len(my_list)}'.")
        self.logger.info(f"Table {table_name} data added in '{stop_time - start_time}' s.")



    def ingest_kv(self, data, table_name):
        """
        Create a table from a python map of key-valye pairs and save to the database with keys as columns

        Args:
            data (map): key-value storagea with the data
            table_name (str): name of the table to create for that CSV file
        """

        start_time = time.time()
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()


        keys_list = list(data.keys())
        values_list = list(data.values())
        cleaned_values_list = [s.split('#')[0].strip() for s in values_list]

        print(len(keys_list), len(cleaned_values_list))
        
        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone() is None:
            # If the table doesn't exist, create it
            cursor.execute(f"CREATE TABLE {table_name} ({', '.join(keys_list)});")
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists. Appending data...")

        placeholders = ', '.join('?' for _ in cleaned_values_list)
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", cleaned_values_list)

        conn.commit()
        conn.close()

        stop_time = time.time()

        print(f"Number of items inserted: '{len(keys_list)}'.")

        self.logger.info(f"ingest_kv: Table {table_name} created in '{stop_time - start_time}' s.")
        self.logger.info(f"  Number of rows inserted: '{len(keys_list)}'.")



    def ingest_as_kv(self, data, table_name):
        """
        Create a table from a python map of key-valye pairs and save to the database as a key-value pair

        Args:
            data (map): key-value storagea with the data
            table_name (str): name of the table to create for that CSV file
        """

        start_time = time.time()
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (key TEXT, value TEXT);")    
        
        num_rows = 0
        for key, value in data.items():
            cursor.execute(f"INSERT OR REPLACE INTO {table_name} (key, value) VALUES (?, ?);", (key, value))
            num_rows = num_rows+1

        conn.commit()
        conn.close()

        stop_time = time.time()

        print(f"Number of rows inserted: '{num_rows}'.")

        
        self.logger.info(f"ingest_kv: Table {table_name} created in '{stop_time - start_time}' s.")
        self.logger.info(f"  Number of rows inserted: '{num_rows}'.")



    def make_primary_key(self, table_name, column_name):
        """
        Create a table from a python map of key-valye pairs and save to the database as a key-value pair

        Args:
            data (map): key-value storagea with the data
            table_name (str): name of the table to create for that CSV file
        """

        start_time = time.time()

        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Get the schema of the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()

        # Check if the specified column exists
        column_names = [col[1] for col in columns_info]
        if column_name not in column_names:
            print(f"Error: Column '{column_name}' does not exist in table '{table_name}'.")
            conn.close()
            return

        # Create the column definitions for the new table
        new_columns = []
        for col in columns_info:
            col_def = f"{col[1]} {col[2]}"
            if col[1] == column_name:
                col_def += " PRIMARY KEY"
            new_columns.append(col_def)
        
        new_columns_str = ", ".join(new_columns)

        # Generate the new table creation SQL statement
        new_table_name = f"{table_name}_new"
        create_new_table_sql = f"CREATE TABLE {new_table_name} ({new_columns_str});"
        
        # Create the new table with the primary key
        cursor.execute(create_new_table_sql)
        #print(f"Created new table '{new_table_name}' with '{column_name}' as the primary key.")

        # Copy data from the old table to the new table
        columns_str = ", ".join(column_names)
        cursor.execute(f"INSERT INTO {new_table_name} ({columns_str}) SELECT {columns_str} FROM {table_name};")
        #print(f"Copied data from '{table_name}' to '{new_table_name}'.")

        # Drop the old table
        cursor.execute(f"DROP TABLE {table_name};")
        #print(f"Dropped old table '{table_name}'.")

        # Rename the new table to the original name
        cursor.execute(f"ALTER TABLE {new_table_name} RENAME TO {table_name};")
        #print(f"Renamed '{new_table_name}' to '{table_name}'.")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        print("Operation completed successfully.")

        stop_time = time.time()

    
        self.logger.info(f"make_primary_key: Table {table_name} modified in '{stop_time - start_time}' s.")




    def query(self, query):
        """
        Get data out from a table

        Args:
            query (str) : query to run on the 

        Returns:
            list of tuples : all the data from the table
        """

        start_time = time.time()

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        try:
            # Try executing the query
            cursor.execute(query)
            results = cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Invalid SQL query: {e}")
            results = None
        
        conn.close()

        stop_time = time.time()
        self.logger.info(f"query: Retreiving data for query took '{stop_time - start_time}' s.")


        return results



    def fetch_all_data(self, table_name):
        """
        Get data out from a table

        Args:
            table_name (str) : name of the table

        Returns:
            list of tuples : all the data from the table
        """

        start_time = time.time()

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone() is None:
            print(f"Table '{table_name}' does not exist.")
            conn.close()
            return None
        

        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        
        conn.close()

        stop_time = time.time()
        self.logger.info(f"fetch_all_data: Retreiving data from '{table_name}' took '{stop_time - start_time}' s.")


        return results



    def list_tables(self):
        """
        List all the tables in the database

        Returns:
            list : names of the tables in the database
        """

        start_time = time.time()
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Query to list all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

        # Fetch and print all table names
        tables = cursor.fetchall()

        table_names = []
        for table in tables:
           table_names.append( table[0] )

        conn.close()

        stop_time = time.time()
        self.logger.info(f"list_tables: Listing tables took '{stop_time - start_time}' s.")


        return table_names



    def get_table_info(self, table_name):
        """
        Get information about a table in the database

        Args:
            table_name (str): name of the table to get infomation about

        Returns:
            int : number of rows in the table
        """

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()


        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if cursor.fetchone() is None:
            print(f"Table '{table_name}' does not exist.")
            conn.close()
            return None

        # Get the schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()

        print(f"Schema of the '{table_name}' table:")
        for column in schema:
            print(f"Column ID: {column[0]}, Name: {column[1]}, Type: {column[2]}, "
                f"Not Null: {column[3]}, Default Value: {column[4]}, Primary Key: {column[5]}")
        
        # Get the number of rows with doata
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        n = cursor.fetchone()[0]

        conn.close()

        return n