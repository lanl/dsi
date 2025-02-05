import sqlite3
import shutil
import os
import logging
import hashlib
import uuid
import time

from db import Store


class ParallelDB:
    """
    A class that allows data to be ingested in parallel in a sqlite database

    Attributes:
        database: the actual database to connect to

    """

    # Class variables
    initialized_params = {}  # count how many times this class has been called
    db_status = {}           # dictionary of object ids and their status


    def __init__(self, db_name: str, n: int, use_mpi: bool = False, mpi_rank: int = 0) -> None:
        """
        Initialize a database

        Args;
            db_name (str): path of the orinal database
            n (int)      : number of parallel shards being used to ingest data in the database
        """

        # Exit is the file does not exist
        if not os.path.isfile(db_name):
            print(f"{db_name} does not exist. Exiting!")
            logging.info(f"{db_name} does not exist. Exiting!")
            return


        # store the number of databases we want
        self.n = n  
        self.db_name = db_name
        self.dbs = []               # list of databases
        self.use_mpi = use_mpi


        # Creating temp databases
        if db_name in ParallelDB.initialized_params:
            pass
        else:
            logging.debug(f"New initialization for {db_name}.")

            ParallelDB.initialized_params[db_name] = 0   # keep track of shard number of database

            # Create n temporary databases
            for i in range(n):
                temp_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{i}.db"    # Create a temporary name
                shutil.copy(db_name, temp_db)           # Create a copy of the databse
                self.dbs.append(temp_db)                # save the name of the database


        # Exit if more databases are created than allowed
        if ParallelDB.initialized_params[db_name] >= n:
            print(f"Too many allocated. Exiting!")
            logging.info(f"Too many allocated. Exiting!")
            return
        

        # Connect to the database
        if use_mpi:
            my_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{ParallelDB.initialized_params[db_name]}.db"
        else:
            my_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{mpi_rank}.db"
        
        self.database = Store( my_db )


        self.uuid = uuid.uuid4().hex
        self.db_status[self.uuid] = 0 # in reading data mode
        #print(self.uuid)

        
        print(f"Starting shard {ParallelDB.initialized_params[db_name] +1} of {n} for {db_name}.")
        logging.debug(f"Starting shard {ParallelDB.initialized_params[db_name] +1} of {n} for {db_name}.")

        # increment counter
        ParallelDB.initialized_params[db_name] = ParallelDB.initialized_params[db_name] + 1





    def all_done(self) -> None:
        """
        To be called by each rank once all the data has been ingested
        """
        
        self.db_status[self.uuid] = 1   # done!

        sum = 0
        for k in self.db_status:
            sum = sum + self.db_status[k]
        

        if sum != self.n:
            # some databases are still reading
            return
        else:
            print("Merging databases")
            self.__merge_tables()



            for file_path in self.dbs:
                if os.path.exists(file_path):   # Check if file exists
                    os.remove(file_path)        # Delete the file
                    print(f"{file_path} deleted successfully")





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

        start_time = time.perf_counter()


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
                #print(f"Skipping duplicate in {table_name}: {row}")
                logging.debug(f"Skipping duplicate in {table_name}: {row}") 
            else:
                #print(f"Inserting new entry in {table_name}: {row}")
                target_cursor.execute(f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})", row)

        # Commit changes and close connections
        target_conn.commit()
        target_conn.close()
        source_conn.close()

        end_time = time.perf_counter()

        #print(f"Merging table {table_name} took {end_time - start_time} s.")
        logging.debug(f"Merging table {table_name} took {end_time - start_time} s.") 
        return 1



    def __merge_tables(self) -> None:
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
            start_time = time.perf_counter()

            #print(f"Merging table {db}")
            for table in tables:
                merge_status = self.__merge_table(self.db_name, db, table)

            end_time = time.perf_counter()
            logging.info(f"Merging {db} took {end_time - start_time} s.") 
            print(f"Merging {db} took {end_time - start_time} s.") 
            



