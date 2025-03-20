import sqlite3
import shutil
import os
import logging
import hashlib
import uuid
import time

from db import Store
from mpi4py import MPI

from db_utils import *

class ParallelDB:
    """
    A class that allows data to be ingested in parallel in a sqlite database

    Attributes:
        database: the actual database to connect to

    """

    # Class variables
    db_counter = {}  # counts how many times this class has been called; for local parallelism
    db_status = {}   # dictionary of object ids and their status; for local parallelism


    def __init__(self, db_name: str, n: int, use_mpi: bool = False, mpi_rank: int = 0) -> None:
        """
        Initialize a database

        Args:
            db_name (str): path of the master/original database
            n (int)      : number of parallel shards being used to ingest data in the database
        """

        # Exit is the file does not exist
        if not os.path.isfile(db_name):
            print(f"{db_name} does not exist. Exiting!")
            logging.info(f"{db_name} does not exist. Exiting!")
            return

        
        self.n = n                  # store the number of databases we want
        self.db_name = db_name      # path of the master/original database

        self.dbs = []               # list of databases; for local parallelism
        self.use_mpi = use_mpi      

        if self.use_mpi:
            self.__init_mpi_parallelism(db_name, n, mpi_rank)
        else:
            self.__init_local_parallelism(db_name, n)




    def all_done(self) -> None:
        """
        To be called by each rank once all the data has been ingested
        """

        if (self.use_mpi):
            self.__all_done_mpi()
        else:
            self.__all_done_local()
        


    def __all_done_local(self) -> None:
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
            print("Merging databases ...")
            self.__merge_tables()


            for file_path in self.dbs:
                if os.path.exists(file_path):   # Check if file exists
                    os.remove(file_path)        # Delete the file
                    logging.debug(f"{file_path} deleted successfully")


    def __all_done_mpi(self) -> None:
        """
        To be called by each rank once all the data has been ingested
        """
        comm = MPI.COMM_WORLD
        if self.mpi_rank != 0:
            req = comm.isend(self.mpi_rank, dest=0, tag=100)
            req.Free()  # Free the request to avoid memory leaks


        if self.mpi_rank == 0:
            completed_ranks = 1  # Rank 0 is automatically done
            while completed_ranks < self.n:
                status = MPI.Status()
                if comm.Iprobe(source=MPI.ANY_SOURCE, tag=100, status=status):
                    msg = comm.recv(source=status.source, tag=100)
                    #print(msg)
                    completed_ranks += 1

            print("Merging databases ...")

            self.__merge_tables()

            for file_path in self.dbs:
                if os.path.exists(file_path):   # Check if file exists
                    os.remove(file_path)        # Delete the file
                    logging.debug(f"{file_path} deleted successfully")


            print("All ranks have completed.")






    def __init_mpi_parallelism(self, db_name: str, n: int, mpi_rank: int ) -> None:
        """
        Initialize parallelism for MPI

        Args:
            db_name (str)  : path of the orinal database
            n (int)        : number of parallel shards being used to ingest data in the database
            mpi_rank (int) : my rank
        """

        comm = MPI.COMM_WORLD
        self.mpi_rank = mpi_rank
        n = comm.Get_size()
        my_temp_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{mpi_rank}.db"    # Create a temporary name
        shutil.copy(db_name, my_temp_db)           # Create a copy of the databse
        self.database = Store( my_temp_db )        # Connect to the DB
        self.db_status = [0] * n                   # initialize the status of all the databases to 0

        self.dbs = comm.gather(my_temp_db, root=0) # gather the names of the temporaty database on rank 0
        

        #print(f"Starting shard {my_temp_db} on rank {mpi_rank} of {n}.")
        logging.debug(f"Starting shard {my_temp_db} on rank {mpi_rank} of {n}.")




    def __init_local_parallelism(self, db_name: str, n: int) -> None:
        """
        Initialize parallelism for local; non MPI

        Args:
            db_name (str): path of the orinal database
            n (int)      : number of parallel shards being used to ingest data in the database
        """

        # Creating temp databases
        if db_name in ParallelDB.db_counter:
            pass
        else:
            logging.debug(f"New initialization for {db_name}.")

            ParallelDB.db_counter[db_name] = 0   # keep track of shard number of database

            # Create n temporary databases
            for i in range(n):
                temp_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{i}.db"    # Create a temporary name
                shutil.copy(db_name, temp_db)           # Create a copy of the databse
                self.dbs.append(temp_db)                # save the name of the database


        # Exit if more databases are created than allowed
        if ParallelDB.db_counter[db_name] >= n:
            print(f"Too many allocated. Exiting!")
            logging.info(f"Too many allocated. Exiting!")
            return
        

        # Connect to the database
        my_db = f"_temp_{hashlib.sha256(db_name.encode()).hexdigest()}_{ParallelDB.db_counter[db_name]}.db"
        self.database = Store( my_db )


        self.uuid = uuid.uuid4().hex
        self.db_status[self.uuid] = 0 # in reading data mode
        #print(self.uuid)

        
        #print(f"Starting shard {ParallelDB.db_counter[db_name] +1} of {n} for {db_name}.")
        logging.debug(f"Starting shard {ParallelDB.db_counter[db_name] +1} of {n} for {db_name}.")

        # increment counter
        ParallelDB.db_counter[db_name] = ParallelDB.db_counter[db_name] + 1






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
        start_time = time.perf_counter()
        for db in self.dbs:
            start_time = time.perf_counter()

            #print(f"Merging table {db}")
            for table in tables:
                merge_status = merge_table(self.db_name, db, table)

            end_time = time.perf_counter()
            logging.info(f"Merging {db} took {end_time - start_time} s.") 
            logging.debug(f"Merging {db} took {end_time - start_time} s.") 

        end_time = time.perf_counter()
        print(f"Merging databases took {end_time - start_time} s.") 
        logging.info(f"Merging databases took {end_time - start_time} s.") 
            