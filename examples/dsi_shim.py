from urllib.parse import urlparse
import urllib.request
import ssl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from db_utils import *


class QueryResult:
    def __init__(self, headers, rows):
        self.headers = headers
        self.rows = rows

    def df(self):
        return pd.DataFrame(self.rows, columns=self.headers)
    

class DSI_Shim:
    '''
    A class for a simplified interface for DSI
    '''

    def __init__(self):
        self.a = Store()
        self.db_initialized = False
        return
    
    def close(self):
        '''
        Close the connection to the database
        '''
        self.a.close()



    def display_table(self, table_name, num_rows=25):
        '''
        Displays the contents of a table

        Args:
            table_name (str): name of the table
            num_rows: number of rows to display
        '''
        results, headers = self.a.query(f"Select * from {table_name}")
        self.__pretty_print(headers, results, num_rows)


    
    def export_table(self, table_name, filename):
        '''
        Exports to a csv/parquet file
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
        '''
        results, headers = self.a.query(sql_query)
        
        extension = filename.rsplit('.', 1)[-1]
        if extension  == "csv":
            self.__output_csv(headers, results, filename)
        elif extension == "pq" or extension == "parquet":
            self.__output_parquet(headers, results, filename)
        else:
            print("Export format not supported!\n")


    def import_dataframe(self, df, table_name):
        '''
        Import a dataframe to the database
        '''
        # Extract headers (columns)
        headers = df.columns.tolist()

        # Extract data (rows)
        data = df.values.tolist()

        self.a.insert_bulk(table_name, headers, data)


    def import_file(self, db_filename):
        '''
        Imports a CSV/parquet file to the current database
        '''
        file_extension = db_filename.rsplit(".", 1)[-1]
        
        if file_extension.lower() == 'csv':
            try:
                self.a.load_csv_to_sqlite(db_filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}") 
                print("Hint: when the database is empty, please use load.\n")
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.a.load_csv_to_sqlite(db_filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}") 
                print("Hint: when the database is empty, please use load.\n")
        else:
            print("This database format is not supported. Currently supported imports formats are: ")
            print("   - parquet (extension: .pq or .parquet)")
            print("   - csv (extension: .csv)\n")



    def list_tables(self, args):
        '''
        Displays information about a database
        '''
        db_info = self.a.get_tables_info()
        for table in db_info:
            print(f"\nTable: {table[0]}")
            print(f"  - num of columns: {table[1]}")
            print(f"  - num of rows: {table[2]}")
        print("\n")
    


    def load(self, db_filename=""):
        '''
        Loads a database
        '''
        if self.db_initialized == True:
            self.import_file(db_filename)
            return
            
        if db_filename == "":
            if os.path.exists(".temp.db"):
                os.remove(".temp.db")
                self.a.connect_to_db(".temp.db")
                self.db_initialized = True
                print(f"Database initialized with no table. Please import some data...\n")
                return

        if self.__is_url(db_filename): # if it's a url, do fetch
            self._fetch(db_filename)
            return
        
        file_extension = db_filename.rsplit(".", 1)[-1]
        if self.a.is_sqlite3_file(db_filename):
            try:
                self.a.connect_to_db(db_filename)
                print(f"Database has {self.a.get_num_tables()} table")
                self.db_initialized = True
                print(f"{db_filename} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}\n")    

        elif file_extension.lower() == 'csv':
            try:
                if os.path.exists(".temp.db"):
                    os.remove(".temp.db")

                self.a.connect_to_db(".temp.db")
                self.a.load_csv_to_sqlite(db_filename)
                print(f"Database has {self.a.get_num_tables()} table")
                self.db_initialized = True
                print(f"{db_filename} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}\n")   
                
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                if os.path.exists(".temp.db"):
                    os.remove(".temp.db")

                self.a.connect_to_db(".temp.db")
                self.a.load_pq_to_sqlite(db_filename)
                print(f"Database has {self.a.get_num_tables()} table")
                self.db_initialized = True
                print(f"{db_filename} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}\n")   

        else:
            print("This database format is not supported. Currently supported formats are: ")
            print("   - csv (extension: .csv)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)\n")



    def query(self, sql_query, dataframe=False, num_rows=25):
        '''
        Runs the query sent to it
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
        '''
        self.a.copy_sqlite_db(filename)
        print(f"Database saved to '{filename}'.\n")


    def summarize(self, table_name=""):
        if table_name == "":
            db_info = self.a.get_tables_info()
            for table in db_info:
                print(f"\nTable: {table[0]}")
                headers, rows = self.a.summarize_table(table[0]) 
                self.__pretty_print(headers, rows, 1000)
                print(f"  - num of rows: {table[2]}\n")
        else:
            headers, rows = self.a.summarize_table(table_name) 
            self.__pretty_print(headers, rows, 1000)



    def __pretty_print(self, headers, rows, max_rows=25):
        '''
        Make the output into a nice table
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
        '''
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)  # write header
            writer.writerows(rows)         # write data

        print(f"Query results exported to '{csv_path}'.\n")


    def __output_parquet(self, headers, data, file_path):
        # Convert to PyArrow Table
        columns = list(zip(*data)) if data else [[] for _ in headers]
        arrays = [pa.array(col) for col in columns]
        table = pa.table(arrays, names=headers)
        pq.write_table(table, file_path)

        print(f"Query results exported to '{file_path}'.\n")


    def __fetch(self, url):
        '''
        Fetches a remote file and opens it
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


    def __import_file(self, db_filename):
        '''
        Imports a CSV/parquet file to the current database
        '''
        file_extension = db_filename.rsplit(".", 1)[-1]
        
        if file_extension.lower() == 'csv':
            try:
                self.a.load_csv_to_sqlite(db_filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}") 
                print("Hint: when the database is empty, please use load.\n")
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.a.load_csv_to_sqlite(db_filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}") 
                print("Hint: when the database is empty, please use load.\n")
        else:
            print("This database format is not supported. Currently supported imports formats are: ")
            print("   - parquet (extension: .pq or .parquet)")
            print("   - csv (extension: .csv)\n")


    def __is_url(self, s: str) -> bool:
        try:
            result = urlparse(s)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False