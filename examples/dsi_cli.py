import shlex
import readline 
from urllib.parse import urlparse
import urllib.request
import importlib.util
import glob
import ssl
import pyarrow as pa
import pyarrow.parquet as pq

from db_utils import *


def check_dependencies(packages):
    missing = []
    for pkg in packages:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    return missing

def is_url(s: str) -> bool:
    try:
        result = urlparse(s)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def is_package_installed(pkg_name: str) -> bool:
    return importlib.util.find_spec(pkg_name) is not None
    

def path_completer(text, state):
    """
    Completes file and directory paths for the current input
    """
    line = readline.get_line_buffer()
    parts = shlex.split(line[:readline.get_endidx()], posix=True)
    
    # Handle the case where the user is starting a new argument
    if line and line[-1].isspace():
        parts.append('')
    
    # Only complete the current argument (the last one)
    if parts:
        curr = parts[-1]
    else:
        curr = ''
    
    # Expand ~ and glob for possible matches
    curr_expanded = os.path.expanduser(curr)
    matches = glob.glob(curr_expanded + '*')

    # Append a slash to directories to hint completion
    matches = [m + '/' if os.path.isdir(m) else m for m in matches]

    # Return the match corresponding to this state
    try:
        return matches[state]
    except IndexError:
        return None

# Enable autocompletion
readline.set_completer_delims(' \t\n')  # So paths with `/` are not broken
readline.set_completer(path_completer)
readline.parse_and_bind('tab: complete')




class DSI_Cli:
    '''
    A class for command line interface for DSI
    '''

    def __init__(self):
        self.a = Store()
        self.db_initialized = False
        return
    

    def help_fn(self, args):
        print("display <table name> [num rows]      Displays the contents of that table, num rows is optional")
        print("exit                                 Exit the DSI Command Line Interface (CLI)")
        print("export_table <table> <filename>      Export the contents of that table to a CSV/parquet file")
        #print("fetch <database url>                 Fetch and loads a remote database")
        print("help                                 Shows this help")
        #print("import <filename>                    Imports file to the current DSI database")
        print("list                                 Lists the tables in the current DSI databse")
        print("load <filename>                      Loads this filename/url to a DSI database")
        print("query <SQL query> [num rows]         Runs a query, num rows is optional")
        print("query_export <SQL query> <filename>  Export the result of thje query to a CSV/parquet file")
        print("save <filename>                      Save the local database as <filename>")
        print("schema <table>                       Get the schema of a table")
        print("summary [table]                      Get a summary of the database or just a table\n")
    
    
    def clear(self, args):
        '''
        Clears the command line
        '''
        os.system('cls' if os.name == 'nt' else 'clear')


    def display(self, args):
        '''
        Displays the contents of a table
        '''
        num_rows = 25
        if (len(args) > 1):
            num_rows = args[1]

        results, headers = self.a.query(f"Select * from {args[0]}")
        self._pretty_print(headers, results, num_rows)


    def exit_cli(self, args):
        '''
        Exits the CLI
        '''
        print("Exiting...")
        exit(0)


    def export_csv(self, args):
        '''
        Exports to a csv/parquet file
        '''
        results, headers = self.a.query(f"Select * from {args[0]}")

        extension = args[1].rsplit('.', 1)[-1]
        if extension  == "csv":
            self._output_csv(headers, results, args[1])
        elif extension == "pq" or extension == "parquet":
            self._output_parquet(headers, results, args[1])
        else:
            print("Export format not supported!\n")
            


    def _fetch(self, args):
        '''
        Fetches a remote file and opens it
        '''
        url = args[0]    
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
    

    def load(self, args):
        '''
        Loads a database
        '''
        if self.db_initialized == True:
            self._import_file(args[0])
            return
            
        if len(args) == 0:
            if os.path.exists(".temp.db"):
                os.remove(".temp.db")
                self.a.connect_to_db(".temp.db")
                self.db_initialized = True
                print(f"Database initialized with no table. Please import some data...\n")
                return

        if is_url(args[0]): # if it's a url, do fetch
            self._fetch(args)
            return
        
        db_filename = args[0]
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
                self.a.load_csv_to_sqlite(args[0])
                print(f"Database has {self.a.get_num_tables()} table")
                self.db_initialized = True
                print(f"{args[0]} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}\n")   
                
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                if os.path.exists(".temp.db"):
                    os.remove(".temp.db")

                self.a.connect_to_db(".temp.db")
                self.a.load_pq_to_sqlite(args[0])
                print(f"Database has {self.a.get_num_tables()} table")
                self.db_initialized = True
                print(f"{args[0]} successfully loaded.\n")
            except Exception as e:
                print(f"An error {e} occurred loading {db_filename}\n")   

        else:
            print("This database format is not supported. Currently supported formats are: ")
            print("   - csv (extension: .csv)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)\n")


    def query(self, args):
        '''
        Runs the query sent to it
        '''
        num_rows = 25
        if (len(args) > 1):
            num_rows = args[1]

        results, headers = self.a.query(args[0])
        self._pretty_print(headers, results, num_rows)
        print("\n")


    def query_export(self, args):
        '''
        Save the result of the query to a csv file
        '''
        results, headers = self.a.query(args[0])
        #self._output_csv(headers, results, args[1])
        
        extension = args[1].rsplit('.', 1)[-1]
        if extension  == "csv":
            self._output_csv(headers, results, args[1])
        elif extension == "pq" or extension == "parquet":
            self._output_parquet(headers, results, args[1])
        else:
            print("Export format not supported!\n")


    def save_to_file(self, args):
        '''
        Save the database to file
        '''
        self.a.copy_sqlite_db(args[0])
        print(f"Database saved to '{args[0]}'.\n")


    def schema(self, args):
        '''
        Get the schema of the database
        '''
        try:
            headers, rows = self.a.get_schema(args[0])
            self._pretty_print(headers, rows, -1)
        except Exception as e:
            print(f"An error {e} occurred getting the schema of table {args[0]}\n")   


    def summary(self, args):
        '''
        Get the summary of a table or database
        '''
        if len(args) > 0:
            print(f"Summary of table {args[0]}:")
        else:
            print(f"Summary for database:")
        
        return 



    def version(self):
        '''
        Output the version of DSI being used
        '''
        print("DSI version 1.1")    # TODO: pull from _version.py
        print("Enter \"help\" for usage hints.\n")


    def _pretty_print(self, headers, rows, max_rows=25):
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

        print("\n")


    def _import_file(self, filename):
        '''
        Imports a CSV/parquet file to the current database
        '''
        file_extension = filename.rsplit(".", 1)[-1]
        
        if file_extension.lower() == 'csv':
            try:
                self.a.load_csv_to_sqlite(filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {filename}\n") 
        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.a.load_pq_to_sqlite(filename)
                print(f"Database now has {self.a.get_num_tables()} table\n")
            except Exception as e:
                print(f"An error {e} occurred loading {filename}\n") 
        else:
            print("This database format is not supported. Currently supported imports formats are: ")
            print("   - csv (extension: .csv)")
            print("   - parquet (extension: .pq, .parquet)\n")
            

    def _output_csv(self, column_names, rows, csv_path):
        '''
        Output as csv
        '''
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)  # write header
            writer.writerows(rows)         # write data

        print(f"Query results exported to '{csv_path}'.\n")


    def _output_parquet(self, headers, data, file_path):
        # Convert to PyArrow Table
        columns = list(zip(*data)) if data else [[] for _ in headers]
        arrays = [pa.array(col) for col in columns]
        table = pa.table(arrays, names=headers)
        pq.write_table(table, file_path)

        print(f"Query results exported to '{file_path}'.\n")



cli = DSI_Cli()


COMMANDS = {
    'clear': cli.clear,
    'display' : cli.display,
    'exit': cli.exit_cli,
    'export_table' : cli.export_csv,
    #'fetch' : cli.fetch,
    'help': cli.help_fn,
    #'import' : cli.import_file,
    'list' : cli.list_tables,
    'load' : cli.load,
    'query' : cli.query,
    'query_export' : cli.query_export,
    'save' : cli.save_to_file,
    'schema' : cli.schema,
    'summary' : cli.summary,
}


def main():
    cli.version()
    
    while True:
        try:
            user_input = input("dsi> ")
            tokens = shlex.split(user_input)
            if not tokens:
                continue

            command, *args = tokens
            if command in COMMANDS:
                COMMANDS[command](args)
            else:
                print(f"Unknown command: {command}")
                print("Type \"help\" to get valid commands.\n")

        except KeyboardInterrupt:
            print("\nUse 'exit' to leave.\n")

        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    main()
