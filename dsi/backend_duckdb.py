import os
import re
import duckdb
import sqlite3
import pandas as pd
import sys


class DSI_Duck:
    def __init__(self):
        return
    
    
    def __del__(self):
        self.close()
        
        
    def close(self):
        self.conn.close()
        
        
    def connect_to_db(self, database):
        self.database = database
        self.conn = duckdb.connect(database)

        
    def save_db(self, dest_path):
        self.conn.execute(f"EXPORT DATABASE '{dest_path}' (FORMAT 'duckdb')")
    
    
    def query(self, sql_query):
        try:
            result = self.conn.execute(sql_query)

            # If query returns data (e.g., SELECT)
            if result.description:
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description]
                #return [dict(zip(columns, row)) for row in rows]
                return rows, columns
            else:
                # For queries like INSERT, CREATE, etc.
                return None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        


    def import_csv(self, csv_path, table_name=None):
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"CSV file '{csv_path}' not found.")

        if table_name is None:
            table_name = os.path.splitext(os.path.basename(csv_path))[0]

        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            if not table_exists:
                # Create table with inferred schema from CSV
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto('{csv_path}', SAMPLE_SIZE=-1)
                """)
                print(f"Table '{table_name}' created.")
                return True

            # Table exists: get table columns
            table_columns = self.conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchdf()['column_name'].tolist()

            # Get CSV columns using describe
            csv_columns = self.conn.execute(f"""
                SELECT column_name
                FROM describe SELECT * FROM read_csv_auto('{csv_path}', SAMPLE_SIZE=-1)
            """).fetchdf()['column_name'].tolist()

            if not set(csv_columns).issubset(set(table_columns)):
                print("Error: CSV columns are not a subset of the existing table schema.")
                print(f"CSV columns: {csv_columns}")
                print(f"Table columns: {table_columns}")
                return False

            # Safe to insert
            column_list = ', '.join(csv_columns)
            self.conn.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list} FROM read_csv_auto('{csv_path}', SAMPLE_SIZE=-1)
            """)
            print(f"Data inserted into existing table '{table_name}'.")
            return True

        except Exception as e:
            print(f"Error ingesting CSV: {e}")
            return False
        

    def import_parquet(self, parquet_path, table_name=None):
        if not os.path.isfile(parquet_path):
            raise FileNotFoundError(f"Parquet file '{parquet_path}' not found.")

        if table_name is None:
            table_name = os.path.splitext(os.path.basename(parquet_path))[0]

        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            if not table_exists:
                # Create table from Parquet
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{parquet_path}')
                """)
                print(f"Table '{table_name}' created.")
                return True

            # Table exists: check column compatibility
            table_columns = self.conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchdf()['column_name'].tolist()

            parquet_columns = self.conn.execute(f"""
                SELECT column_name
                FROM describe SELECT * FROM read_parquet('{parquet_path}')
            """).fetchdf()['column_name'].tolist()

            if not set(parquet_columns).issubset(set(table_columns)):
                print("Error: Parquet columns are not a subset of the existing table schema.")
                print(f"Parquet columns: {parquet_columns}")
                print(f"Table columns: {table_columns}")
                return False

            # Safe to insert
            column_list = ', '.join(parquet_columns)
            self.conn.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list} FROM read_parquet('{parquet_path}')
            """)
            print(f"Data inserted into existing table '{table_name}'.")
            return True

        except Exception as e:
            print(f"Error ingesting Parquet: {e}")
            return False

    
    def import_sqlite(self, sqlite_db_path):

        #def import_sqlite_to_duckdb(sqlite_db_path, duckdb_db_path="my_duckdb.db"):
        """
        Imports all tables from a SQLite database into a DuckDB database.

        Args:
        sqlite_db_path (str): The path to the SQLite database file.
        duckdb_db_path (str): The path to the DuckDB database file (optional).
        """

        # Connect to the DuckDB database
        self.conn.execute("INSTALL sqlite;")  # Install the sqlite extension if not already installed
        self.conn.execute("LOAD sqlite;") # Load the extension
        
        try:
            # Attach the SQLite database
            self.conn.execute(f"ATTACH '{sqlite_db_path}' AS sqlite_db (TYPE sqlite);")
            
            # Get a list of tables from the SQLite database
            sqlite_conn = sqlite3.connect(sqlite_db_path)
            cursor = sqlite_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            sqlite_tables = [row[0] for row in cursor.fetchall()]
            sqlite_conn.close()
            
            #self.conn.execute("DETACH sqlite_db;")
            
            
            if not sqlite_tables:
                print("No tables found in the SQLite database.")
                return False
            

            # Create DuckDB tables and import data
            for table_name in sqlite_tables:
                # table_exists = self.conn.execute(f"""
                #     SELECT COUNT(*) > 0
                #     FROM information_schema.tables
                #     WHERE table_schema = 'main' AND table_name = '{table_name}'
                # """).fetchone()[0]

                # "SELECT * FROM duckdb_tables() WHERE database_name <> 'sqlite_db'"
                row = self.conn.execute(f"""
                    SELECT COUNT(*) 
                    FROM duckdb_tables()
                    WHERE database_name <> 'sqlite_db' AND table_name = '{table_name}'
                """).fetchone()

                table_exists = row[0] if row else False
                

                
                if table_exists:
                    # Get schema of existing DuckDB table
                    duckdb_schema = self.conn.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'main' AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                        
                    """).fetchall()

                    # Get schema of SQLite table
                    sqlite_schema = self.conn.execute(f"""
                        PRAGMA table_info('sqlite_db.{table_name}')
                    """).fetchall()

                    if duckdb_schema == sqlite_schema:
                        # Insert if schemas match
                        self.conn.execute(f"""
                            INSERT INTO {table_name}
                            SELECT * FROM sqlite_db.{table_name}
                        """)
                        print(f"Data inserted into existing table '{table_name}'.")
                    else:
                        print(f" Schema mismatch for table '{table_name}'. Skipping insert.")
                        print(f"  DuckDB schema : {duckdb_schema}")
                        print(f"  SQLite schema : {sqlite_schema}")
                else:
                    self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM sqlite_db.{table_name};")
            
            print("Finished importing all tables!")
            
            # Detatch sqlite table
            self.conn.execute("DETACH sqlite_db;")

        except Exception as e:
            print(f"An error occurred: {e}")


            
    def import_duckdb(self, source_path):
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"DuckDB file '{source_path}' not found.")

        try:
            # Attach the source DuckDB file as another schema
            self.conn.execute(f"ATTACH DATABASE '{source_path}' AS source_db")

            # Get list of tables in the source database
            tables = self.conn.execute("""
                SELECT table_name FROM source_db.information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()

            if not tables:
                print("No tables found in the source DuckDB database.")
                return False

            for (table_name,) in tables:
                # Check if table already exists in the target database
                table_exists = self.conn.execute(f"""
                    SELECT COUNT(*) > 0
                    FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = '{table_name}'
                """).fetchone()[0]

                if table_exists:
                    # Get schema of target table
                    target_schema = self.conn.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'main' AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """).fetchall()

                    # Get schema of source table
                    source_schema = self.conn.execute(f"""
                        SELECT column_name, data_type
                        FROM source_db.information_schema.columns
                        WHERE table_schema = 'main' AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """).fetchall()

                    if target_schema == source_schema:
                        # Insert data if schema matches
                        self.conn.execute(f"""
                            INSERT INTO {table_name}
                            SELECT * FROM source_db.{table_name}
                        """)
                        print(f" Data inserted into existing table '{table_name}'.")
                    else:
                        print(f"  Schema mismatch for table '{table_name}'. Skipping insert.")
                        print(f"  Target schema: {target_schema}")
                        print(f"  Source schema: {source_schema}")
                else:
                    # Table does not exist: create it
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS
                        SELECT * FROM source_db.{table_name}
                    """)
                    print(f" Table '{table_name}' created.")

            return True

        except Exception as e:
            print(f" Error during DuckDB-to-DuckDB ingestion: {e}")
            return False


    def import_dataframe(self, df, table_name):
        if not isinstance(df, pd.DataFrame):
            print("Provided data is not a valid pandas DataFrame.")
            return False

        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            # Register DataFrame
            self.conn.register('temp_df', df)

            if not table_exists:
                # Table doesn't exist: create it directly
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                print(f"Table '{table_name}' created.")
                return True

            # Table exists: check column compatibility
            existing_columns = self.conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchdf()['column_name'].tolist()

            df_columns = df.columns.tolist()

            # Ensure all DataFrame columns exist in the table
            if not set(df_columns).issubset(set(existing_columns)):
                print("DataFrame columns do not match or are not a subset of the existing table.")
                print(f"Existing table columns: {existing_columns}")
                print(f"DataFrame columns: {df_columns}")
                return False

            # Columns match (as subset): insert only matching columns
            column_list = ', '.join(df_columns)
            self.conn.execute(f"""
                INSERT INTO {table_name} ({column_list})
                SELECT {column_list} FROM temp_df
            """)
            print(f"Data inserted into existing table '{table_name}'.")

            return True

        except Exception as e:
            print(f"Error ingesting DataFrame: {e}")
            return False


    def import_dict(self, data, table_name):
        if not isinstance(data, dict):
            print("Provided data is not a valid dictionary.")
            return False

        df = pd.DataFrame(data)
        return self.ingest_dataframe(df,table_name)


    def import_list(self, rows, columns, table_name):
        if not isinstance(data, dict):
            print("Provided data is not a valid dictionary.")
            return False

        df = pd.DataFrame(rows, columns=columns)
        return self.ingest_dataframe(df,table_name)


    def execute_sql_script(self, sql_input, is_file):
        try:
            # Load SQL from file or string
            if is_file:
                if not os.path.isfile(sql_input):
                    print(f"SQL file not found: {sql_input}")
                    return False
                with open(sql_input, 'r') as f:
                    sql_script = f.read()
            else:
                sql_script = sql_input

            if not sql_script.strip():
                print("SQL script is empty.")
                return False


            # Split into individual statements
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

            for stmt in statements:
                # Detect CREATE TABLE statements (basic regex; assumes standard syntax)
                match = re.match(r"(?i)^CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)", stmt)
                if match:
                    table_name = match.group(2)

                    # Check if table already exists
                    exists = self.conn.execute(f"""
                        SELECT COUNT(*) > 0
                        FROM information_schema.tables
                        WHERE table_schema = 'main' AND table_name = '{table_name}'
                    """).fetchone()[0]

                    if exists:
                        print(f"Table '{table_name}' already exists. Skipping CREATE statement.")
                        continue

                # Execute the SQL statement
                self.conn.execute(stmt)

            print("SQL script executed successfully.")
            return True

        except Exception as e:
            print(f"Error executing SQL script: {e}")
            return False

    
    def get_tables_info(self):
        """
        Retrieves a summary of all tables in the DuckDB database:
        - table name
        - number of columns
        - number of rows

        Returns:
            list of [table_name, num_columns, num_rows]
        """
        try:
            # Step 1: Get all user-defined tables in the main schema
            tables = self.conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            """).fetchall()

            db_info = []

            for (table_name,) in tables:
                # Get number of columns
                num_columns = self.conn.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = '{table_name}'
                """).fetchone()[0]

                # Get number of rows
                num_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

                db_info.append([table_name, num_columns, num_rows])

            return db_info

        except Exception as e:
            print(f"Error retrieving table info: {e}")
            return []
    
    
    def get_table_shape(self, table_name):
        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            if not table_exists:
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Get number of columns
            num_columns =  self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            # Get number of rows
            num_rows =  self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            return num_columns, num_rows

        except Exception as e:
            print(f"Error querying table shape: {e}")
            return None
        
        
    def list_tables(self):
        try:
            result = self.conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()

            return [row[0] for row in result]

        except Exception as e:
            print(f"Error listing tables: {e}")
            return []
        
        
    def get_num_tables(self):
        """
        Returns the number of user-defined tables in the DuckDB database.

        Returns:
            int: Number of tables in the 'main' schema.
        """
        try:
            return self.conn.execute("""
                SELECT COUNT(*) 
                FROM duckdb_tables() 
                WHERE schema_name = 'main'
            """).fetchone()[0]

        except Exception as e:
            print(f"Error getting number of tables: {e}")
            return None    
    
    
    def get_table_schema(self, table_name):
        """
        Returns the schema of a DuckDB table in a format similar to SQLite's PRAGMA table_info.

        Returns:
            headers: List of column header labels
            rows: List of column metadata [Column, Type, Not Null, PK]
        """
        try:
            # Check if the table exists
            exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            if not exists:
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Query column info
            result = self.conn.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_name IN (
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'main' 
                        AND table_name = '{table_name}' 
                        AND column_name IN (
                            SELECT column_name 
                            FROM duckdb_constraints()
                            WHERE constraint_type = 'PRIMARY KEY' AND table_name = '{table_name}'
                        )
                    ) AS is_primary_key
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()

            headers = ["Column", "Type", "Not Null", "PK"]
            rows = []

            for name, dtype, is_nullable, is_pk in result:
                rows.append([
                    str(name),
                    str(dtype),
                    str(is_nullable == 'NO'),  # "Not Null" if nullable is NO
                    str(bool(is_pk))
                ])

            return headers, rows

        except Exception as e:
            print(f"Error retrieving schema for table '{table_name}': {e}")
            return None
        
        
    def summarize_table(self, table_name):
        """
        Compute column statistics from a DuckDB table and return results as headers and row data.

        Parameters
        ----------
        table_name : str
            Name of the DuckDB table to analyze.

        Returns
        -------
        tuple
            A tuple containing:
                - headers: list of column headers
                - rows: list of rows, each as a list of values
        """
        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

            if not table_exists:
                print(f"Table '{table_name}' does not exist.")
                return None

            # Get column info
            columns_info = self.conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'main' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()

            # Detect primary key columns
            pk_columns = set()
            pk_result = self.conn.execute(f"""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_name = kcu.table_name
                WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
            """).fetchall()
            pk_columns = {row[0] for row in pk_result}

            # Define summary output
            headers = ['column', 'type', 'min', 'max', 'avg', 'std_dev']
            rows = []

            numeric_types = {'INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'REAL'}

            for col_name, col_type in columns_info:
                col_type_upper = col_type.upper()
                is_numeric = any(nt in col_type_upper for nt in numeric_types)
                is_primary = col_name in pk_columns
                display_name = f"{col_name}*" if is_primary else col_name

                if is_numeric:
                    # Compute stats using DuckDB SQL
                    query = f"""
                        WITH stats AS (
                            SELECT AVG("{col_name}") AS mean
                            FROM {table_name}
                            WHERE "{col_name}" IS NOT NULL
                        )
                        SELECT 
                            MIN("{col_name}"),
                            MAX("{col_name}"),
                            AVG("{col_name}"),
                            CASE 
                                WHEN COUNT("{col_name}") > 1 THEN 
                                    SQRT(AVG(POWER("{col_name}" - stats.mean, 2)))
                                ELSE NULL
                            END AS std_dev
                        FROM {table_name}, stats
                        WHERE "{col_name}" IS NOT NULL
                    """
                    min_val, max_val, avg_val, std_dev = self.conn.execute(query).fetchone()
                else:
                    min_val = max_val = avg_val = std_dev = None

                rows.append([display_name, col_type, min_val, max_val, avg_val, std_dev])

            return headers, rows

        except Exception as e:
            print(f"Error summarizing table '{table_name}': {e}")
            return None
        
        
    def search_database(self, search_string):
        """
        Searches across all tables, column names, and text column values
        in the DuckDB database for a given search string.

        Parameters:
            search_string (str): The string to search for.

        Returns:
            dict: {
                'tables': [matching_table_names],
                'columns': [{'table': ..., 'column': ...}, ...],
                'rows': [{'table': ..., 'column': ..., 'row': ...}, ...]
            }
        """
        results = {
            'tables': [],
            'columns': [],
            'rows': []
        }

        try:
            search_lower = search_string.lower()

            # Get all table names from the main schema
            tables = self.conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            """).fetchdf()['table_name'].tolist()

            for table in tables:
                # Match table name
                if search_lower in table.lower():
                    results['tables'].append(table)

                # Get column names and types
                columns_info = self.conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'main' AND table_name = '{table}'
                """).fetchall()

                for col_name, col_type in columns_info:
                    # Match column name
                    if search_lower in col_name.lower():
                        results['columns'].append({'table': table, 'column': col_name})

                    # Only search in text-compatible columns
                    if col_type.lower() in ('text', 'varchar', 'string'):
                        try:
                            matches = self.conn.execute(f"""
                                SELECT * FROM {table}
                                WHERE {col_name} ILIKE '%{search_string}%'
                                LIMIT 100
                            """).fetchall()

                            for row in matches:
                                results['rows'].append({
                                    'table': table,
                                    'column': col_name,
                                    'row': row
                                })
                        except Exception:
                            continue

            return results

        except Exception as e:
            print(f"Error searching DuckDB: {e}")
            return results
        
        
    def table_exists(self, table_name):
        table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = '{table_name}'
            """).fetchone()[0]

        return table_exists