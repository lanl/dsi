import sqlite3
import logging
import time

class Store:
    """
    Create a storage for data, eventually will be replaced by DSI

    Attributes:
        db_name (str): name of the database
    """

    def __init__(self, path: str = None) -> None:
        """
        Initializes the storage class
        """
        if path != None:
            self.connect_to_db(path)


    def close(self) -> None:
        """
        Close connection to the database
        """
        self.conn.close()



    def connect_to_db(self, path: str) -> None:
        """
        Create or connect to a database with the path specified

        Args:
            path (str) : path to where to create or db to connect to
        """
        print(f"path: {path}")
        self.conn = sqlite3.connect(path)

        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")


        # Fetch and print all table names
        tables = cursor.fetchall()
        logging.debug(f"List of tables: '{tables}'")
        logging.info(f"Database '{path}' connected to.")



    def insert_col_data(self, table_name: str, columns: list[str], data: list[list]) -> None:
        """
        Insert data into a table with column names provided

        Args:
            table_name (str) : name of the table to use
            columns (list[list]) : headers in a list
            data (list[list]) : data in a list of lists
        """
        columns_str = ', '.join(columns)  
        placeholders = ', '.join('?' for _ in data)

        cursor = self.conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});", data)
        self.conn.commit()
        cursor.close()



    def insert_data(self, table_name: str, data: list[list]) -> None:
        """
        Insert data into a table with just the data, risky!!!

        Args:
            table_name (str) : name of the table to use
            data (list[list]) : data in a list of lists
        """
        placeholders = ', '.join('?' for _ in data)
        
        cursor = self.conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", data)
        self.conn.commit()
        cursor.close()



    def add_colums(self, table_name: str, column_names: list[str], column_types: list[list]) -> None:
        """
        Add columns to a table

        Args:
            table_name (str) : name of the table to use
            column_names (list[str]) : data in a list of lists
            column_types (list[list]) : types in a list of lists
        """

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




    def check_if_col_exists(self, table_name: str, column_name: str) -> bool:
        """
        Check if column exists

        Args:
            table_name (str) : name of table
            column_name (str): name of column

        Returns:
            boolean : true if column exists
        """

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")

        columns = cursor.fetchall()
        column_exists = any(col[1] == column_name for col in columns)

        cursor.close()

        if column_exists:
            return True
        else:
            return False
        



    def query(self, query: str) -> list[tuple]:
        """
        Get data out from a table

        Args:
            query (str) : query to run on the 

        Returns:
             list[tuple] : all the data from the table
        """

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
        logging.debug(f"query: Retreiving data for query took '{stop_time - start_time}' s.")

        cursor.close()

        return results
    



    def exec_script(self, script: str) -> None:
        """
        Execute a sql script

        Args:
            script (str) : script to run
        """

        start_time = time.time()
        cursor = self.conn.cursor()

        try:
            # Try executing the query
            cursor.executescript(script)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Invalid SQL query: {e}")
        

        stop_time = time.time()
        logging.debug(f"query: Retreiving data for query took '{stop_time - start_time}' s.")

        cursor.close()



