import sqlite3
import logging
import os


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
        cursor.close()
        return

    cursor.close()
    print(f"{target_db} created!")
    logging.info(f"{target_db} created!")
    