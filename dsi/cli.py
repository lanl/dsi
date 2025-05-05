import shlex
import readline 
import glob
import argparse
import os
import shutil
import pandas as pd
from collections import OrderedDict

from dsi.core import Terminal
from ._version import __version__

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


class DSI_cli:
    '''
    A class for command line interface for DSI
    '''
    t = []

    def __init__(self):
        return
    
    def startup(self, backend="sqlite"):
        self.t = Terminal(debug = 0, runTable=False)
        self.name = None
        if backend=="duckdb":
            if os.path.exists(".temp.duckdb"):
                os.remove(".temp.duckdb")
            self.t.load_module('backend','DuckDB','back-write', filename=".temp.duckdb")
            self.name = ".temp.duckdb"
        else:
            if os.path.exists(".temp.sqlite"):
                os.remove(".temp.sqlite")
            self.t.load_module('backend','Sqlite','back-write', filename=".temp.sqlite")
            self.name = ".temp.sqlite"
        return
    

    def help_fn(self, args):
        print("display <table name> [-n num rows] [-e filename]  Displays the contents of that table, num rows to display is ")
        print("                                                      optional, and it can be exported to a csv/parquet file")
        print("exit                                              Exit the DSI Command Line Interface (CLI)")
        print("draw [-f filename]                                Draws an ER Diagram of all tables in the current DSI database")
        print("find <var>                                        Search for a variable in the dataset")
        print("help                                              Shows this help")
        print("list                                              Lists the tables in the current DSI databse")
        print("load <filename> [-t table name]                   Loads this filename/url to a DSI database. optional")
        print("                                                      table name argument if input file is only one table")
        print("plot_table <table_name> [-f filename]             Plots a table's numerical data to an optional file name argument")
        print("query <SQL query> [-n num rows] [-e filename]     Runs a query (in quotes), displays an optionl num rows")
        print("                                                      , and exports output to a csv/parquet file")
        print("save <filename>                                   Save the local database as <filename>, which will be the same type.")
        print("summary [-t table] [-n num_rows]                  Get a summary of the database or just a table and optionally ")
        print("                                                     specify number of data rows to display\n")
        print("ls                                                Lists all files in current directory or a specified path")
        print("cd <path>                                         Changes the working directory within the CLI environment")

    def cd(self, args):
        '''
        Changes the current working directory only within the CLI environment
        '''
        if not args:
            print("Usage: cd <directory>")
            return
        path = os.path.expanduser(args[0])
        try:
            os.chdir(path)
            print(f"Changed directory to {os.getcwd()}")
        except FileNotFoundError:
            print(f"No such directory: {path}")
        except NotADirectoryError:
            print(f"{path} is not a directory.")
        except Exception as e:
            print(f"Error: {e}")

    def clear(self, args):
        '''
        Clears the command line
        '''
        os.system('cls' if os.name == 'nt' else 'clear')

    
    def get_display_parser(self):
        parser = argparse.ArgumentParser(prog='display')
        parser.add_argument('table_name', help='Table to display')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows  for each table')
        parser.add_argument('-e', '--export', type=str, required=False, help='Export to csv or parquet file')
        return parser

    def display(self, args):
        '''
        Displays the contents of a table
        '''
        print(f"table_name: {args.table_name}")
        
        table_name = args.table_name
        num_rows = 25
        if args.num_rows != None:
            num_rows = args.num_rows

        self.t.display(table_name, num_rows)
        
        if args.export != None:
            file_extension = args.export.rsplit(".", 1)[-1] if '.' in args.export else ''
            if file_extension.lower() not in ["csv", "pq", "parquet"]:
                filename = args.export + ".csv"
            else:
                filename = args.export
            self.export_table(table_name, filename)
    
    def get_draw_parser(self):
        parser = argparse.ArgumentParser(prog='draw')
        parser.add_argument('-f', '--filename', type=str, required=False, help='Show only this table')
        return parser
    
    def draw_schema(self, args):
        '''
        Generates an ER diagram from all data loaded in
        '''
        erd_name = "er_diagram.png"
        if args.filename != None:
            erd_name = args.filename
        
        self.export_table("dsi_erd_gen", erd_name)

    def exit_cli(self, args):
        '''
        Exits the CLI
        '''
        print("Exiting...")
        self.t.close()
        exit(0)

    def export_table(self, table_name, filename):
        '''
        Exports to a csv/parquet file
        '''
        if table_name != "temp_query":
            self.t.artifact_handler(interaction_type="process")

        file_extension = filename.rsplit(".", 1)[-1] if '.' in filename else ''
        if table_name == "dsi_erd_gen":
            self.t.load_module('plugin', "ER_Diagram", "writer", filename = filename)
            self.t.transload()
        elif "dsi_tb_" in table_name:
            self.t.load_module('plugin', "Table_Plot", "writer", table_name = table_name[7:], filename = filename)
            self.t.transload()
        elif file_extension.lower() == "csv":
            self.t.load_module('plugin', "Csv_Writer", "writer", filename = filename, table_name = table_name)
            self.t.transload()
        elif file_extension.lower() in ['pq', 'parquet']:
            table_data = self.t.active_metadata[table_name]
            df = pd.DataFrame(table_data)
            df.to_parquet(filename, engine='pyarrow', index=False)
        
        self.t.active_metadata = OrderedDict()
        
    def find(self, args):
        '''
        Global find to see where that string exists
        '''
        find_list = self.t.find(args[0])
        print()
        for val in find_list:
            print(f"Table: {val.t_name}")
            print(f"  - Column(s): {val.c_name}")
            print(f"  - Search Type: {val.type}")
            print(f"  - Row Number: {val.row_num}")
            print(f"  - Value: {val.value}")
        print()
              
    def list_tables(self, args):
        '''
        Lists the tables in the database
        '''
        self.t.list()
    

    def get_load_parser(self):
        parser = argparse.ArgumentParser(prog='load')
        parser.add_argument('filename', help='File to load ito DSI')
        parser.add_argument('-t', '--table_name', type=str, required=False, default="", help='table name of csv or parquet file')
        return parser
    
    def load(self, args):
        '''
        Loads data to into a DSI database or loads a DSI database

        Args:
            dbfile (obj): name of the file or database to load 
            table_name (str): name of the table to load the data to for CSV and parquet
        '''
        table_name = ""
        if args.table_name != "":
            table_name = args.table_name
        else:
            table_name = os.path.splitext(os.path.basename(args.filename))[0]

            
        dbfile = args.filename

        if self.__is_url(dbfile): # if it's a url, do fetch
            url = dbfile
            output_path = url.split('/')[-1]    

            try:
                import ssl
                import urllib.request
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

        file_extension = dbfile.rsplit(".", 1)[-1] if '.' in dbfile else ''
        if self.__is_sqlite3_file(dbfile):
            try:
                self.t.load_module('backend','Sqlite','back-read', filename=dbfile)
                self.t.artifact_handler(interaction_type="process")
                self.t.unload_module('backend','Sqlite','back-read')
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")    

        elif self.__is_duckdb_file(dbfile):
            try:
                self.t.load_module('backend','DuckDB','back-read', filename=dbfile)
                self.t.artifact_handler(interaction_type="process")
                self.t.unload_module('backend','DuckDB','back-read')
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")  

        elif file_extension.lower() == 'csv':
            # handle ALL files that should end in CSV: Csv and Wildfire for now.
            try:
                self.t.load_module('plugin', "Csv", "reader", filenames = dbfile, table_name = table_name)
            except Exception as e:
                try:
                    self.t.load_module('plugin', "Wildfire", "reader", filenames = dbfile, table_name = table_name)
                except Exception as e:
                    print(f"An error occured loading {dbfile} into DSI. Please ensure your data file structure is correct\n")
        
        elif file_extension.lower() == 'toml':
            try:
                self.t.load_module('plugin', "TOML1", "reader", filenames = dbfile)
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")

        elif file_extension.lower() in ['yaml', 'yml']:
            # handle ALL files that should end in yaml: YAML1 and Oceans11 for now.
            try:
                self.t.load_module('plugin', "YAML1", "reader", filenames = dbfile)
            except Exception as e:
                try:
                    self.t.load_module('plugin', "Oceans11Datacard", "reader", filenames = dbfile)
                except Exception as e:
                    print(f"An error occured loading {dbfile} into DSI. Please ensure your data file structure is correct\n")

        elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
            try:
                self.t.load_module('backend','Parquet','back-write', filename=dbfile)
                data = OrderedDict(self.t.artifact_handler(interaction_type="query")) #Parquet's query() returns a normal dict
                if table_name is not None:
                    self.t.active_metadata[table_name] = data
                else:
                    self.t.active_metadata["Parquet"] = data
                self.t.unload_module('backend','Parquet','back-write')
            except Exception as e:
                print(f"An error {e} occurred loading {dbfile}\n")   

        if self.t.active_metadata:
            self.t.artifact_handler(interaction_type='ingest')
            self.t.num_tables()
            self.t.active_metadata = OrderedDict()
            print(f"{dbfile} successfully loaded.\n")
        else:
            print("Ensure file has data stored correctly.")
            print("Currently supported formats are: ")
            print("   - csv (extension: .csv)")
            print("   - toml (extension: .toml)")
            print("   - yaml (extension: .yaml, .yml)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)")
            print("   - duckdb (extension: .duckdb, .db)\n")
    
    def ls(self, args):
        '''
        Lists contents of the current directory or specified path
        '''
        path = args[0] if args else '.'
        try:
            entries = [entry for entry in os.listdir(path) if not entry.startswith('.')]
            for entry in entries:
                full_path = os.path.join(path, entry)
                suffix = '/' if os.path.isdir(full_path) else ''
                print(entry + suffix)
        except FileNotFoundError:
            print(f"No such file or directory: {path}")
        except Exception as e:
            print(f"Error: {e}")

    def get_plot_table_parser(self):
        parser = argparse.ArgumentParser(prog='plot_table')
        parser.add_argument('table_name', help='Table to plot')
        parser.add_argument('-f', '--filename', type=str, required=False, default="", help='Export filename')
        return parser
    
    def plot_table(self, args):
        '''
        Plot a table's numerical data and store in an image
        '''
        table_name = args.table_name
        filename = f"{table_name}_plot.png"
        if args.filename != "":
            filename = args.filename
        
        self.export_table("dsi_tb_" + table_name, filename)

    def get_query_parser(self):
        parser = argparse.ArgumentParser(prog='display')
        parser.add_argument('sql_query', help='SQL query to execute')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows  for each table')
        parser.add_argument('-e', '--export', type=str, required=False, help='Export to csv or parquet file')
        return parser

    def query(self, args):
        '''
        Runs the query sent to it
        '''
        sql_query = args.sql_query
        num_rows = 25
        if args.num_rows != None:
            num_rows = args.num_rows

        data = self.t.artifact_handler(interaction_type='query', query = sql_query).head(num_rows)
        
        pd.set_option('display.max_rows', 1000)
        print(data)
        pd.reset_option('display.max_rows')
        
        if args.export != None:
            file_extension = args.export.rsplit(".", 1)[-1] if '.' in args.export else ''
            if file_extension.lower() not in ["csv", "pq", "parquet"]:
                filename = args.export + ".csv"
            else:
                filename = args.export
            self.t.active_metadata["temp_query"] = OrderedDict(data.to_dict(orient='list'))
            self.export_table("temp_query", filename)

    def get_save_parser(self):
        parser = argparse.ArgumentParser(prog='save')
        parser.add_argument('filename', help='file DSI data will be saved to')
        return parser
    
    def save_to_file(self, args):
        '''
        Save the database to file
        '''
        new_name = args.filename
        file_extension = new_name.rsplit(".", 1)[-1] if '.' in new_name else ''
        if "sqlite" in self.name:
            if file_extension.lower() in ["db", "sqlite", "sqlite3"]:
                shutil.copyfile(self.name, new_name)
            else:
                shutil.copyfile(self.name, new_name + ".sqlite")
        elif "duckdb" in self.name:
            if file_extension.lower() in ["db", "duckdb"]:
                shutil.copyfile(self.name, new_name)
            else:
                shutil.copyfile(self.name, new_name + ".duckdb")

    def get_summary_parser(self):
        parser = argparse.ArgumentParser(prog='summary')
        parser.add_argument('-t', '--table', type=str, required=False, help='Show only this table')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows for each table')
        return parser
    
    def summary(self, args):
        '''
        Get the summary of a table or database
        '''
        table_name = None
        if args.table != None:
            table_name = args.table
           
        num_rows = 0
        if args.num_rows != None:
            num_rows = args.num_rows
        
        #self.a.summarize(table_name, num_rows)
        self.t.summary(table_name, num_rows)


    def version(self):
        '''
        Output the version of DSI being used
        '''
        print("DSI version " + str(__version__)+"\n")
        print("Enter \"help\" for usage hints.\n")


    # TODO: Abstract later to __is_valid_file and have independent checks in the dsi.backends
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
    
    def __is_url(self, s):
        '''
        Checks if the string is a url link
        
        Args:
            s (str): string to check
        '''
        try:
            from urllib.parse import urlparse
            result = urlparse(s)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False


cli = DSI_cli()

COMMANDS = {
    'clear': (None, cli.clear), #
    'display' : (cli.get_display_parser, cli.display), #
    'draw' : (cli.get_draw_parser, cli.draw_schema),
    'exit': (None, cli.exit_cli), #
    'find' : (None, cli.find),
    'help': (None, cli.help_fn), #
    'list' : (None, cli.list_tables), #
    'load' : (cli.get_load_parser, cli.load),
    'plot_table' : (cli.get_plot_table_parser, cli.plot_table),
    'query' : (cli.get_query_parser, cli.query),
    'save' : (cli.get_save_parser, cli.save_to_file),
    'summary' : (cli.get_summary_parser, cli.summary), #
    'ls' : (None, cli.ls), #
    'cd' : (None, cli.cd) #
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--backend", type=str, default="sqlite", help="Supported backends are sqlite and duckdb")

    args = parser.parse_args()
    cli.version()
    cli.startup(args.backend)

    while True:
        try:
            user_input = input("dsi> ")
            tokens = shlex.split(user_input)
            if not tokens:
                continue

            command, *args = tokens
            
            if command not in COMMANDS:
                print(f"Unknown command: {command}")
                print("Type \"help\" to get valid commands.\n")
                
            parser_factory, handler = COMMANDS[command]

            if parser_factory:
                parser = parser_factory()
                try:
                    parsed_args = parser.parse_args(args)
                    handler(parsed_args)
                except SystemExit:
                    # argparse tries to exit on error — suppress that in shell
                    pass
            else:
                handler(args)
            
        except KeyboardInterrupt:
            print("\nUse 'exit' to leave.\n")

        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    main()
