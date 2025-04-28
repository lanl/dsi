from urllib.parse import urlparse
import urllib.request
import ssl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backend_sqlite import *  # TODO: Replace by DSI interface
from backend_duckdb import *


class DSI_Shim:
    '''
    A class for a simplified interface for DSI
    '''

    def __init__(self, backend="sqlite"):
        
        self.backend =  backend
        if backend == "duckdb":
            self.a = DSI_Duck()
            
            if os.path.exists(".temp.duckdb"):
                os.remove(".temp.duckdb")
            self.a.connect_to_db(".temp.duckdb")
            
        else:
            self.a = DSI_Sqlite()
            
            if os.path.exists(".temp.sqlite"):
                os.remove(".temp.sqlite")
            self.a.connect_to_db(".temp.sqlite")
        
        return
     
    
    def __del__(self):
        self.close()
        
    
    def close(self):
        '''
        Close the connection to the database
        '''
        self.a.close()
        
        
        # Remove the temporay file
        if os.path.exists(".temp.sqlite"):
            os.remove(".temp.sqlite")
            
        # Remove the temporay file
        if os.path.exists(".temp.duckdb"):
            os.remove(".temp.duckdb")


    def display_table(self, table_name, num_rows=25):
        '''
        Displays the contents of a table

        Args:
            table_name (str): name of the table
            num_rows (int): number of rows to display
        '''
        if not self.a.table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return
        
        results, headers = self.a.query(f"Select * from {table_name}")
        self.__pretty_print(headers, results, num_rows)
        



    def export_table(self, table_name, filename):
        '''
        Exports to a csv/parquet file
        
        Args:
            table_name (str): name of the table
            filename (str): name of the file to export to table contents to
        '''
        if not self.a.table_exists(table_name):
            print(f"Table {table_name} does not exist!")
            return
        
        results, headers = self.a.query(f"Select * from {table_name}")
        
        extension = filename.rsplit('.', 1)[-1]
        if extension  == "csv":
            self.__output_csv(headers, results, filename)
        elif extension == "pq" or extension == "parquet":
            self.__output_parquet(headers, results, filename)
        else:
            print("Export format not supported!\n")
            

    def export_query(self, sql_query, filename):
        '''
        Save the result of the query to a csv or parquet file
        
        Args:
            sql_query (str): sql query to execute on the database
            filename (str): name of the file to export to the results of the query to
        '''
        results, headers = self.a.query(sql_query)
        
        extension = filename.rsplit('.', 1)[-1]
        if extension  == "csv":
            self.__output_csv(headers, results, filename)
        elif extension == "pq" or extension == "parquet":
            self.__output_parquet(headers, results, filename)
        else:
            print("Export format not supported!\n")


    def find(self, x):
        '''
        A general find that displays and return the results of the query
        
        Args:
            x (str): the string to find in the table
        '''        
        results = self.a.search_database(x)
        self.__print_results(results)
            


    def list_tables(self):
        '''
        List the tables in the database
        '''
        db_info = self.a.get_tables_info()
        rows = []
        for table in db_info:
            row = []
            row.append(table[0])
            row.append(table[1])
            row.append(table[2])
            rows.append(row)
        header = ['Table Name', '# columns', '# data rows']
        
        self.__pretty_print(header, rows, -1)
    


    def load(self, dbfile, table_name=""):
        '''
        Loads file to a database
        
        Args:
            dbfile (obj): name of the file to load or dataframe
            table_name (str): name of the table to load the data to for CSV and parquet
        '''
        if self.__is_url(dbfile): # if it's a url, do fetch
            url = dbfile
            output_path = url.split('/')[-1]    

            try:
                # Use certifi's trusted certificate bundle
                context = ssl._create_unverified_context()

                # Use urlopen instead of urlretrieve
                with urllib.request.urlopen(url, context=context) as response:
                    with open(output_path, 'wb') as out_file:
                        out_file.write(response.read())
                
                print(f"File downloaded successfully as {output_path}")
                dbfile = output_path

            except Exception as e:
                print(f"Download failed: {e}")
                return
        
        
        if isinstance(dbfile, pd.DataFrame):
            self.a.import_dataframe(dbfile, table_name)
            return
        
        file_extension = dbfile.rsplit(".", 1)[-1]
        if self.__is_sqlite3_file(dbfile):
            try:
                self.a.import_sqlite(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")    
                
        elif self.__is_duckdb_file(dbfile):
            try:
                self.a.import_duckdb(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")  

        elif file_extension.lower() == 'csv':
            try:
                self.a.import_csv(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")   
                
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.a.import_parquet(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")   

        else:
            print("This database format is not supported. Currently supported formats are: ")
            print("   - csv (extension: .csv)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)\n")



    def query(self, sql_query, dataframe=False, num_rows=25):
        '''
        Runs the query sent to it
        
        Args:
            sql_query (str): the sql query to run
            dataframe (bool): whether to return a pandas dataframe or not
            num_rows (int): number of rows to display
        '''
        start = time.time()
        results, headers = self.a.query(sql_query)
        end = time.time()
        elapsed_seconds = end - start
        print(f"Query returned {len(results)} and took {elapsed_seconds}s")

        if (dataframe == True):
            return pd.DataFrame(results, columns=headers)
        else:
            self.__pretty_print(headers, results, num_rows)
            print("\n")



    def save_to_file(self, filename):
        '''
        Save the database to file
        
        Args:
            filename (str): the file to save the data to
        '''
        self.a.save_db(filename)
        print(f"Database saved to '{filename}'.\n")



    def summarize(self, table_name="", num_rows=0):
        '''
        Displays a summary of the table if provided or the full database
        
        Args:
            table_name (str): the name of the table to summary (optional)
            num_rows (int): the number of rows to display, 0 means don't show
        '''
        if table_name == "":
            db_info = self.a.get_tables_info()
            for table in db_info:
                print(f"\nTable: {table[0]}")
                headers, rows = self.a.summarize_table(table[0]) 
                self.__pretty_print(headers, rows, 1000)
                
                if num_rows > 0:
                    self.display_table(table[0], num_rows)
                else:
                    print(f"  - num of rows: {table[2]}\n")
        else:
            headers, rows = self.a.summarize_table(table_name) 
            self.__pretty_print(headers, rows, 1000)
            
            r, _ = self.a.query(f"SELECT COUNT(*) FROM {table_name}")
            
            if num_rows > 0:
                self.display_table(table_name, num_rows)
            else:
                print(f"  - num of rows: {r[0][0]}\n")



    def __pretty_print(self, headers, rows, max_rows=25):
        '''
        Make the output into a nice table
        
        Args:
            headers (list): the list of headers
            rows (list of list): the actual data
            max_rows (int): the number of rows to display
        '''
        # Determine max width for each column
        col_widths = [
            max(
                len(str(h)),
                max((len(str(r[i])) for r in rows if i < len(r)), default=0)
            )
            for i, h in enumerate(headers)
        ]

        # Print header
        header_row = " | ".join(f"{headers[i]:<{col_widths[i]}}" for i in range(len(headers)))
        print("\n" + header_row)
        print("-" * len(header_row))

        # Print each row
        count = 0
        for row in rows:
            print(" | ".join(
                f"{str(row[i]):<{col_widths[i]}}" for i in range(len(headers)) if i < len(row)
            ))

            count += 1
            if count == max_rows:
                print(f"  ... showing {max_rows} of {len(rows)} rows")
                break




    def __output_csv(self, column_names, rows, csv_path):
        '''
        Output as csv
        
        Args:
            column_names (list): the names of the columns
            rows (list of list): the data
            csv_path (str): the name of the to output to
        '''
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)  # write header
            writer.writerows(rows)         # write data

        print(f"Query results exported to '{csv_path}'.\n")


    def __output_parquet(self, headers, data, file_path):
        '''
        Output as a parquet file
        
        Args:
            headers (list): the names of the columns
            data (list of list): the data
            file_path (str): the name of the to output to
        '''
        
        # Convert to PyArrow Table
        columns = list(zip(*data)) if data else [[] for _ in headers]
        arrays = [pa.array(col) for col in columns]
        table = pa.table(arrays, names=headers)
        pq.write_table(table, file_path)

        print(f"Query results exported to '{file_path}'.\n")



    def __is_url(self, s):
        '''
        Checks if the string is a url link
        
        Args:
            s (str): string to check
        '''
        try:
            result = urlparse(s)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False
        
        
    def __is_sqlite3_file(self, filename):
        if not os.path.isfile(filename):
            return False
        
        with open(filename, 'rb') as f:
            header = f.read(16)
        return header == b'SQLite format 3\x00'
    
    
    def __is_duckdb_file(self, file_path):
        if not os.path.isfile(file_path):
            return False

        try:
            with open(file_path, 'rb') as f:
                header = f.read(4)
                return header == b'DUCK'
        except Exception as e:
            print(f"Error reading file: {e}")
            return False
        
        
    def __print_results(self, results):
        for key, items in results.items():
            print("\n")
            if items:  # Skip empty lists
                print(f"{key}:")
                for item in items:
                    if isinstance(item, dict):
                        for sub_key, sub_val in item.items():
                            print(f"    {sub_key}: {sub_val}")
                    else:
                        print(f"    {item}")
                    