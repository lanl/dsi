from urllib.parse import urlparse
import urllib.request
import ssl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from db_utils import *  # TODO: Replace by DSI interface

## Functions needed from DSI:
# close(): close connection to db
# query(str): sql query of database
# search_database(str): string to query in database
# get_tables_info(str): find info about a table
# get_num_tables(): get the number of tables in a database
# copy_sqlite_db(filename): dave to file
# summarize_table(str): summarize a table 
# import_database(dbfile)
# load_csv_to_sqlite(dbfile)
# load_pq_to_sqlite(dbfile)
# insert_bulk(table_name, headers, data): insert columns and list of lists

class DSI_Shim:
    '''
    A class for a simplified interface for DSI
    '''

    def __init__(self):
        self.a = Store()
        
        # Create a temporary database to store data
        if os.path.exists(".temp.db"):
            os.remove(".temp.db")
        self.a.connect_to_db(".temp.db")
            
        return
    
    
    def __del__(self):
        self.close()
        
    
    def close(self):
        '''
        Close the connection to the database
        '''
        self.a.close()
        
        # Remove the temporay file
        if os.path.exists(".temp.db"):
            os.remove(".temp.db")


    def display_table(self, table_name, num_rows=25):
        '''
        Displays the contents of a table

        Args:
            table_name (str): name of the table
            num_rows (int): number of rows to display
        '''
        results, headers = self.a.query(f"Select * from {table_name}")
        self.__pretty_print(headers, results, num_rows)


    def export_table(self, table_name, filename):
        '''
        Exports to a csv/parquet file
        
        Args:
            table_name (str): name of the table
            filename (str): name of the file to export to table contents to
        '''
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
        for table in db_info:
            print(f"\nTable: {table[0]}")
            print(f"  - num of columns: {table[1]}")
            print(f"  - num of rows: {table[2]}")
        print("\n")
    


    def load(self, dbfile, table_name=""):
        '''
        Loads file to a database
        
        Args:
            dbfile (obj): name of the file to load or dataframe
            table_name (str): name of the table to load the data to for CSV and parquet
        '''
        if self.__is_url(dbfile): # if it's a url, do fetch
            self.__fetch(dbfile)
            return
        
        if isinstance(dbfile, pd.DataFrame):
            self.__import_dataframe(dbfile, table_name)
            return
        
        file_extension = dbfile.rsplit(".", 1)[-1]
        if self.__is_sqlite3_file(dbfile):
            try:
                self.a.import_database(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")    

        elif file_extension.lower() == 'csv':
            try:
                self.a.load_csv_to_sqlite(dbfile)
                print(f"Database has {self.a.get_num_tables()} table")
                print(f"{dbfile} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")   
                
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.a.load_pq_to_sqlite(dbfile)
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
        results, headers = self.a.query(sql_query)
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
        self.a.copy_sqlite_db(filename)
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


    def __fetch(self, url):
        '''
        Fetches a remote file and opens it
        
        Args:
            url (string): url of the database to retreive
        '''
        output_path = url.split('/')[-1]    

        try:
        # Use certifi's trusted certificate bundle
            context = ssl._create_unverified_context()

            # Use urlopen instead of urlretrieve
            with urllib.request.urlopen(url, context=context) as response:
                with open(output_path, 'wb') as out_file:
                    out_file.write(response.read())
            
            print(f"File downloaded successfully as {output_path}")
            
            self.load([output_path])
        except Exception as e:
            print(f"Download failed: {e}")


    def __import_dataframe(self, df, table_name):
        '''
        Imports a dataframe to the database
        
        Args:
            df (pandas): pandas dataframe to import
            table_name (str): name of the table to import the dataframe to
        '''
        headers = df.columns.tolist()
        data = df.values.tolist()

        self.a.insert_bulk(table_name, headers, data)


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
                    