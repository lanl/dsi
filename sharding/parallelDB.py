import sqlite3
import shutil
import sys
import logging
import random
import string

from db import Store



def random_string(length: int) -> str:
    """
    Generates a random string of letters with the given length.
    """
    letters = string.ascii_letters  # 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join(random.choices(letters, k=length))


class ParallelDB:
    """
    A class that allows data to be ingested in parallel in a sqlite database

    Attributes:
        db_name (str): The path of the original database
        dbs (list)   : list of parallel databases

    """

    
    dbs = []           # list of databases
    initialized_params = {}     # count how many times this class has been called


    def __init__(self, db_name: str, n: int) -> None:
        """
        Initialize a database

        Args;
            db_name (str): path of the orinal database
            n (int)      : number of parallel ranks being used to ingest data in the database
        """

        
        if db_name in ParallelDB.initialized_params:
            print("Already initilized")
            pass
        else:
            print("New initialization")
            # Save the name of the database
            

            ParallelDB.initialized_params[db_name] = 0   # add this parameter

            # Create n temporary databases
            for i in range(n):
                temp_db = f"_temp_db_{i}.db"
                shutil.copy(db_name, temp_db)           # Create a copy of the databse
                self.dbs.append(f"_temp_db_{i}.db")     # save the name of the database


        # Connect to the database
        self.database = Store( self.dbs[ ParallelDB.initialized_params[db_name] ] )
        ParallelDB.initialized_params[db_name] = ParallelDB.initialized_params[db_name] + 1



    def __get_columns(self, cursor: sqlite3.Cursor, table_name: str) -> list[str]:
        """
        Retrieve column names for a given table.

        Args:
            cursor (sqlite3.Cursor): reference to the database
            table_name (str): name of the table

        Return:
            list: list of columns in the table
        """
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]  # Extract column names



    def __merge_table(self, target_db: str, source_db: str, table_name: str) -> int:
        """
        Merge a table from source_db into target_db, avoiding exact duplicates.

        Args:
            target_db (str) : destination to merge
            source_db (str) : origin database
            table_name (str) :name of the table

        Return:
            int: status; 1 is success and 0 is failure
        """

        target_conn = sqlite3.connect(target_db)
        target_cursor = target_conn.cursor()
        
        source_conn = sqlite3.connect(source_db)
        source_cursor = source_conn.cursor()

        # Get column names dynamically
        source_columns = self.__get_columns(source_cursor, table_name)
        target_columns = self.__get_columns(target_cursor, table_name)

        # Ensure both tables have the same structure
        if set(source_columns) != set(target_columns):
            print(f"Schema mismatch: {table_name} has different columns in source and target.")
            logging.error(f"Schema mismatch: {table_name} has different columns in source and target.")
            return 0

        # Create placeholders for SQL query (e.g., ?, ?, ?)
        placeholders = ", ".join(["?"] * len(source_columns))
        column_names_str = ", ".join(source_columns)

        # Select all data from the source table
        source_cursor.execute(f"SELECT {column_names_str} FROM {table_name}")
        rows = source_cursor.fetchall()

        for row in rows:
            # Check if an exact match exists in the target database
            conditions = " AND ".join([f"{col} = ?" for col in source_columns])
            target_cursor.execute(f"SELECT * FROM {table_name} WHERE {conditions}", row)
            exact_match = target_cursor.fetchone()

            if exact_match:
                print(f"Skipping duplicate in {table_name}: {row}")
                logging.debug(f"Skipping duplicate in {table_name}: {row}") 
            else:
                #print(f"Inserting new entry in {table_name}: {row}")
                target_cursor.execute(f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})", row)

        # Commit changes and close connections
        target_conn.commit()
        target_conn.close()
        source_conn.close()

        print(f"Merging for {table_name} completed.")
        logging.into(f"Merging for {table_name} completed.") 
        return 1



    def merge_tables(self):
        """
        Merge all the tables in a database
        """

        # Get the list of tables
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()

        # Merge the tables
        for db in self.dbs:
            print(f"Merging table {db}")
            for table in tables:
                merge_status = self.__merge_table(self.db_name, db, table)



