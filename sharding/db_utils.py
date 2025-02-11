import sqlite3
import logging
import os
import time


def create_sqlite_db(sql_database_template: str, target_db: str) -> None:
    """
    Create a sqlite database from a sqlite schema  file (sql statements)

    Args:
        sql_database_template (str): the schema file (.sql) extension
        target_db (str): the target .db file
    """

    # Check if schema file exists
    if not os.path.isfile(sql_database_template):
        print(f"{sql_database_template} does not exist. Exiting!")
        logging.info(f"{sql_database_template} does not exist. Exiting!")
        return

    with open(sql_database_template, 'r') as file:
        sql_commands = file.read()

    conn = sqlite3.connect(target_db)
    cursor = conn.cursor()

    try: 
        cursor.executescript(sql_commands)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Invalid SQL query: {e}")
        logging.error(f"Invalid SQL query: {sql_commands}, error: {e}")
        cursor.close()
        return

    cursor.close()
    print(f"{target_db} created!")
    logging.info(f"{target_db} created!")
    


def get_columns(cursor: sqlite3.Cursor, table_name: str) -> list[str]:
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




def merge_table(target_db: str, source_db: str, table_name: str) -> int:
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
    source_columns = get_columns(source_cursor, table_name)
    target_columns = get_columns(target_cursor, table_name)

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
            logging.debug(f"Skipping duplicate in {table_name}: {row}") 
        else:
            try:
                target_cursor.execute(f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})", row)
            except sqlite3.Error as e:  # Catch database errors
                print(f"Inserting new entry in {table_name}: {row}")
                logging.error(f"Error inserting data: {e}")
                
    # Commit changes and close connections
    target_conn.commit()
    target_conn.close()
    source_conn.close()


    end_time = time.perf_counter()

    #print(f"Merging table {table_name} took {end_time - start_time} s.")
    logging.debug(f"Merging table {table_name} took {end_time - start_time} s.") 
    return 1