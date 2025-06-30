import shlex
import readline 
import glob
import argparse
import os
import shutil
import pandas as pd
from collections import OrderedDict
import textwrap
from contextlib import redirect_stdout
import sys
import re
import io

from dsi.core import Terminal
from ._version import __version__

def autofill_path(text, state):
    """ Completes file and directory paths for the current input """
    line = readline.get_line_buffer()
    parts = shlex.split(line[:readline.get_endidx()], posix=True)
    
    if line and line[-1].isspace():
        parts.append('')
    
    curr = parts[-1] if parts else ''
    
    matches = glob.glob(os.path.expanduser(curr) + '*')
    matches = [m + '/' if os.path.isdir(m) else m for m in matches]
    matches = [m[m[:-1].rfind('/')+1:] if '/' in m[:-1] else m for m in matches]

    try:
        return matches[state]
    except IndexError:
        return None

readline.set_completer(autofill_path)
if "libedit" in readline.__doc__:
    readline.parse_and_bind("bind ^I rl_complete")
else:
    readline.parse_and_bind("tab: complete")


class DSI_cli:
    '''
    A class for command line interface for DSI
    '''
    t = []

    def __init__(self):
        self.name = None
        self.start_dir = os.getcwd() + "/"
        return
    
    def startup(self, backend="sqlite"):
        self.t = Terminal(debug = 0, runTable=False)
        self.t.user_wrapper = True

        self.start_dir = os.getcwd()
        db_path = os.path.join(self.start_dir, ".temp.db")
        if os.path.exists(db_path):
            os.remove(db_path)

        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                if backend=="duckdb":
                    self.t.load_module('backend','DuckDB','back-write', filename = db_path)
                    self.name = "duckdb"
                else:
                    backend = "sqlite"
                    self.t.load_module('backend','Sqlite','back-write', filename = db_path)
                    self.name = "sqlite"
        except Exception as e:
            print(f"backend ERROR: {e}")
            with redirect_stdout(fnull):
                self.exit_cli()
        print(f"Created a temporary {backend} DSI backend")


    def help_fn(self, args):
        commands = [
            ("display <table_name> [-n num_rows] [-e filename]", 
            "Displays a table's data. Optionally limit displayed rows and export to CSV/Parquet"),
            ("draw [-f filename]", "Draws an ER diagram of all tables in the current DSI database"),
            ("exit", "Exits the DSI Command Line Interface (CLI)"),
            ("find <variable>", "Searches for a variable in DSI"),
            ("help", "Shows this help message."),
            ("list", "Lists all tables in the current DSI database"),
            ("plot_table <table_name> [-f filename]", "Plots numerical data from a table to an optional file name argument"),
            ("query <SQL_query> [-n num_rows] [-e filename]",
            "Executes a SQL query (in quotes). Optionally limit printed rows or export to CSV/Parquet"),
            ("read <filename> [-t table_name]", "Reads a file or URL into the DSI database. Optionally set table name."),
            ("summary [-t table_name]", "Summary of the database or a specific table."),
            ("write <filename>", "Writes data in DSI database to a permanent location."),
            ("ls", "Lists all files in the current or specified directory."),
            ("cd <path>", "Changes the working directory within the CLI environment.")
        ]

        print()
        terminal_width = shutil.get_terminal_size().columns
        for cmd, desc in commands:
            print(textwrap.fill(f"{cmd:48} {desc}", width=terminal_width, subsequent_indent=' ' * 50))
        print()


    def cd(self, args):
        '''
        Changes the current working directory only within the CLI environment
        '''
        if not args:
            print("Usage: cd <directory>")
            return
        path = os.path.expanduser(args[0])
        if os.path.isdir(path):
            os.chdir(path)
            print(f"Changed directory to {os.getcwd()}")
        else:
            print(f"{path} is not a directory.")
        print()


    def clear(self, args):
        '''
        Clears the command line
        '''
        os.system('cls' if os.name == 'nt' else 'clear')

    
    def get_display_parser(self):
        parser = argparse.ArgumentParser(prog='display')
        parser.add_argument('table_name', help='Table to display')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show first n rows of the table')
        parser.add_argument('-e', '--export', type=str, required=False, help='Export to csv or parquet file')
        return parser

    def display(self, args):
        '''
        Displays the contents of a table
        '''     
        table_name = args.table_name
        num_rows = 25
        if args.num_rows != None:
            num_rows = args.num_rows

        try:
            self.t.display(table_name, num_rows)
        except Exception as e:
            print(f"display ERROR: {e}")
        
        if args.export != None:
            file_extension = args.export.rsplit(".", 1)[-1] if '.' in args.export else ''
            if file_extension.lower() not in ["csv", "pq", "parquet"]:
                filename = args.export + ".csv"
            else:
                filename = args.export
            error = self.export_table(table_name, filename)
            if error != 1:
                print(f"Exported {table_name} to {filename}")
            print()
    
    
    def get_draw_parser(self):
        parser = argparse.ArgumentParser(prog='draw')
        parser.add_argument('-f', '--filename', type=str, required=False, help='ER Diagram filename')
        return parser
    
    def draw_schema(self, args):
        '''
        Generates an ER diagram from all data loaded in
        '''
        erd_name = "er_diagram.png"
        if args.filename != None:
            erd_name = args.filename
        
        error = self.export_table("dsi_erd_gen", erd_name)
        if error != 1:
            print(f"Successfully drew an ER Diagram in {erd_name}")
        print()


    def exit_cli(self, args):
        '''
        Exits the CLI
        '''
        print("Exiting...")
        fnull = open(os.devnull, 'w')
        with redirect_stdout(fnull):
            self.t.close()
        exit(0)


    def export_table(self, table_name, filename):
        '''
        Exports to a csv/parquet file
        '''
        if table_name != "temp_query":
            try:        
                self.t.artifact_handler(interaction_type='process')
            except Exception as e:
                self.t.active_metadata = OrderedDict()
                print(f"export ERROR: {e}")
                return 1

        file_extension = filename.rsplit(".", 1)[-1] if '.' in filename else ''
        success_load = True

        if "/" not in filename:
            filename = os.path.join(self.start_dir, filename)

        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                if table_name == "dsi_erd_gen":
                    self.t.load_module('plugin', "ER_Diagram", "writer", filename = filename)
                elif "dsi_tb_" in table_name:
                    self.t.load_module('plugin', "Table_Plot", "writer", table_name = table_name[7:], filename = filename)
                elif file_extension.lower() == "csv":
                    self.t.load_module('plugin', "Csv_Writer", "writer", filename = filename, table_name = table_name)
                elif file_extension.lower() in ['pq', 'parquet']:
                    table_data = self.t.active_metadata[table_name]
                    df = pd.DataFrame(table_data)
                    df.to_parquet(filename, engine='pyarrow', index=False)
                else:
                    success_load = False
        except Exception as e:
            self.t.active_metadata = OrderedDict()
            print(f"export ERROR: {e}")
            return 1
        
        if success_load == True:
            try:
                self.t.transload()
            except Exception as e:
                self.t.active_modules['writer'].pop(0)
                self.t.active_metadata = OrderedDict()
                print(f"export ERROR: {e}")
                return 1
        
        self.t.active_metadata = OrderedDict()
    
    
    def find(self, args):
        '''
        Global find to see where that string exists
        '''
        if not args:
            print("find ERROR: need to specify an object to find")
            return
        if len(args) > 1:
            print("find ERROR: need to wrap a multi-word search in quotes. Ex: find 'this is'")
            return
        query = args[0]

        new_find = False
        operators = ['==', '!=', '>=', '<=', '=', '<', '>', '(']
        if isinstance(query, str) and any(op in query for op in operators):
            pattern = r'(==|!=|>=|<=|=|<|>|\()'
            parts = re.split(r'(".*?")', query)

            result = []
            for part in parts:
                if part.startswith('"') and part.endswith('"'):
                    result.append(part)
                else:
                    result.extend([p.strip() for p in re.split(pattern, part) if p.strip()])
            if len(result) > 1: # can split into column and operator
                new_find = True
                print(f"Finding all rows where '{query}' in the active backend")

                output = None
                try:
                    f = io.StringIO()
                    with redirect_stdout(f):
                        find_list = self.t.find_relation(query)
                    output = f.getvalue()
                except Exception as e:
                    sys.exit(f"find() ERROR: {e}")
                
                if output and "WARNING" in output:
                    warn_msg = output[output.find("WARNING"):]
                    if "artifact_handler" in warn_msg:
                        lines = warn_msg.splitlines()
                        start = lines[1].find('`')
                        between = lines[1][start + 1 : lines[1].find('`', start + 1)]
                        lines[1] = lines[1].replace(between, "query")
                        lines[2] = lines[2].replace(lines[2][lines[2].find('artifact'):-1], "query")
                        warn_msg = '\n'.join(lines)
                    print("\n"+warn_msg.replace("database", "backend"))
                    return

        if new_find == False:
            val = f"'{query}'" if isinstance(query, str) else query
            print(f"Finding all instances of {val} in DSI")
            
            fnull = open(os.devnull, 'w')
            try:
                with redirect_stdout(fnull):
                    find_list = self.t.find_cell(query, row=True)
            except Exception as e:
                print(f"find ERROR: {e}")
                return
        if find_list is None:
            val = f"'{query}'" if isinstance(query, str) else query
            print(f"WARNING: {val} was not found")
            return
        print()
        for val in find_list:
            print(f"Table: {val.t_name}")
            print(f"  - Columns: {val.c_name}")
            print(f"  - Row Number: {val.row_num}")
            print(f"  - Data: {val.value}")
        print()
    

    def list_tables(self, args):
        '''
        Lists the tables in the database
        '''
        try:
            self.t.list()
        except Exception as e:
            print(f"list ERROR: {e}")


    def ls(self, args):
        '''
        Lists contents of the current directory or specified path
        '''
        path = args[0] if args else '.'
        try:
            files = [file for file in os.listdir(path) if not file.startswith('.')]
            files = [file + '/' if os.path.isdir(os.path.join(path, file)) else file for file in files]
            files.sort()

            try:
                term_width = os.get_terminal_size().columns
            except OSError:
                term_width = 80
            col_width = max((len(file) for file in files), default=0) + 3
            num_cols = max(1, term_width // col_width)
            num_rows = (len(files) + num_cols - 1)// num_cols

            for row in range(num_rows):
                line = ''
                for col in range(num_cols):
                    idx = col * num_rows + row
                    if idx < len(files):
                        line += files[idx].ljust(col_width)
                print(line.rstrip())
            print()
        except FileNotFoundError:
            print(f"No such filepath: {path}")
            return
        except Exception as e:
            print(f"Error: {e}")
            return


    def get_plot_table_parser(self):
        parser = argparse.ArgumentParser(prog='plot_table')
        parser.add_argument('table_name', help='Table to plot')
        parser.add_argument('-f', '--filename', type=str, required=False, default="", help='Table plot filename')
        return parser
    
    def plot_table(self, args):
        '''
        Plot a table's numerical data and store in an image
        '''
        table_name = args.table_name
        filename = f"{table_name}_plot.png"
        if args.filename != "":
            filename = args.filename
        
        error = self.export_table("dsi_tb_" + table_name, filename)
        if error != 1:
            print(f"Successfully plotted {table_name} in {filename}")
        print()


    def get_query_parser(self):
        parser = argparse.ArgumentParser(prog='query')
        parser.add_argument('sql_query', help='SQL query (in quotes) to execute')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show first n rows of the table')
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

        print(f"Printing the result from input SQL query: {sql_query}")

        try:
            data = self.t.artifact_handler(interaction_type='query', query = sql_query)
        except Exception as e:
            print(f"query ERROR: {e}")
            return
        if data.empty:
            print()
            return
        
        headers = data.columns.tolist()
        rows = data.values.tolist()
        self.t.table_print_helper(headers, rows, num_rows)
        
        if args.export != None:
            file_extension = args.export.rsplit(".", 1)[-1] if '.' in args.export else ''
            if file_extension.lower() not in ["csv", "pq", "parquet"]:
                filename = args.export + ".csv"
            else:
                filename = args.export
            self.t.active_metadata["temp_query"] = OrderedDict(data.to_dict(orient='list'))
            error = self.export_table("temp_query", filename)
            if error != 1:
                print()
                print(f"Exported the query result to {filename}")
        print()


    def get_read_parser(self):
        parser = argparse.ArgumentParser(prog='read')
        parser.add_argument('filename', help='File to read into DSI')
        parser.add_argument('-t', '--table_name', type=str, required=False, default="", help='table name to store data into')
        return parser
    
    def read(self, args):
        '''
        Reads data file or a database into DSI

        Args:
            dbfile (obj): name of the file or database to read 
            table_name (str): name of the table to read the data into
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

        if not os.path.exists(dbfile):
            print("read ERROR: The input file must be a valid filepath. Please check again.")
            return

        file_extension = dbfile.rsplit(".", 1)[-1] if '.' in dbfile else ''
        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                if self.__is_sqlite3_file(dbfile):
                    self.t.load_module('backend','Sqlite','back-read', filename=dbfile)
                    self.t.artifact_handler(interaction_type="process")
                    self.t.unload_module('backend','Sqlite','back-read')
                elif self.__is_duckdb_file(dbfile):
                    self.t.load_module('backend','DuckDB','back-read', filename=dbfile)
                    self.t.artifact_handler(interaction_type="process")
                    self.t.unload_module('backend','DuckDB','back-read')
                elif file_extension.lower() == 'csv':
                    self.t.load_module('plugin', "Csv", "reader", filenames = dbfile, table_name = table_name)
                elif file_extension.lower() == 'toml':
                    self.t.load_module('plugin', "TOML1", "reader", filenames = dbfile)
                elif file_extension.lower() in ['yaml', 'yml']:
                    self.t.load_module('plugin', "YAML1", "reader", filenames = dbfile)
                elif file_extension.lower() == 'json':
                    self.t.load_module('plugin', "JSON", "reader", filenames = dbfile)
                elif file_extension.lower() == 'pq' or file_extension.lower() == 'parquet':
                    self.t.load_module('backend','Parquet','back-write', filename=dbfile)
                    data = OrderedDict(self.t.artifact_handler(interaction_type="query")) #Parquet's query() returns a normal dict
                    if table_name is not None:
                        self.t.active_metadata[table_name] = data
                    else:
                        self.t.active_metadata["Parquet"] = data
                    self.t.unload_module('backend','Parquet','back-write')
        except Exception as e:
            print(f"read ERROR: {e}\n")
            self.t.active_metadata = OrderedDict()
            return

        if self.t.active_metadata:
            try:
                self.t.artifact_handler(interaction_type='ingest')
            except Exception as e:
                print(f"read ERROR: {e}")
                self.t.active_metadata = OrderedDict()
                return
            
            table_keys = [k for k in self.t.active_metadata if k not in ("dsi_relations", "dsi_units")]
            if len(table_keys) > 1:
                print(f"Loaded {dbfile} into tables: {', '.join(table_keys)}")
            else:
                print(f"Loaded {dbfile} into the table {table_keys[0]}")

            self.t.num_tables()
            print()
            self.t.active_metadata = OrderedDict()
        else:
            print()
            print("Ensure file has data stored correctly.")
            print("Currently supported formats are: ")
            print("   - csv (extension: .csv)")
            print("   - json (extension: .json)")
            print("   - toml (extension: .toml)")
            print("   - yaml (extension: .yaml, .yml)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)")
            print("   - duckdb (extension: .duckdb, .db)\n")
            return


    def get_summary_parser(self):
        parser = argparse.ArgumentParser(prog='summary')
        parser.add_argument('-t', '--table', type=str, required=False, help='Show only this table')
        return parser
    
    def summary(self, args):
        '''
        Get the summary of a table or database
        '''
        table_name = None
        if args.table != None:
            table_name = args.table
        
        try:
            self.t.summary(table_name)
        except Exception as e:
            print(f"summary ERROR: {e}")
        print()


    def version(self):
        '''
        Output the version of DSI being used
        '''
        return str(__version__)


    def get_write_parser(self):
        parser = argparse.ArgumentParser(prog='write')
        parser.add_argument('filename', help='file DSI data will be written to')
        return parser
    
    def write_to_file(self, args):
        '''
        Writes the database to file
        '''
        new_name = args.filename
        file_extension = new_name.rsplit(".", 1)[-1] if '.' in new_name else ''
        dsi_db_path = os.path.join(self.start_dir, ".temp.db")
        final_name = None
        if "sqlite" == self.name:
            if file_extension.lower() in ["db", "sqlite", "sqlite3"]:
                shutil.copyfile(dsi_db_path, os.path.join(self.start_dir, new_name))
                final_name = new_name
            else:
                shutil.copyfile(dsi_db_path, os.path.join(self.start_dir, new_name) + ".sqlite")
                final_name = new_name + ".sqlite"
        elif "duckdb" == self.name:
            if file_extension.lower() in ["db", "duckdb"]:
                shutil.copyfile(dsi_db_path, os.path.join(self.start_dir, new_name))
                final_name = new_name
            else:
                shutil.copyfile(dsi_db_path, os.path.join(self.start_dir, new_name) + ".duckdb")
                final_name = new_name + ".duckdb"
        print(f"Sucessfully wrote all data to {final_name}\n")


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
    'clear': (None, cli.clear),
    'display' : (cli.get_display_parser, cli.display),
    'draw' : (cli.get_draw_parser, cli.draw_schema),
    'exit': (None, cli.exit_cli),
    'find' : (None, cli.find),
    'help': (None, cli.help_fn),
    'list' : (None, cli.list_tables),
    'read' : (cli.get_read_parser, cli.read),
    'plot_table' : (cli.get_plot_table_parser, cli.plot_table),
    'query' : (cli.get_query_parser, cli.query),
    'write' : (cli.get_write_parser, cli.write_to_file),
    'summary' : (cli.get_summary_parser, cli.summary),
    'ls' : (None, cli.ls),
    'cd' : (None, cli.cd)
}

def main():
    if sys.argv[1:] and sys.argv[1].lower() == "help":
        cli.help_fn([])
        exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--backend", type=str, default="sqlite", help="Supported backends are sqlite and duckdb")

    args = parser.parse_args()
    if args.backend.lower() not in ["sqlite", "duckdb"]:
        print("ERROR: Invalid backend input. Valid backends are: sqlite, duckdb")
        exit(1)
    print("   ", textwrap.dedent(fr"""
         _____           ___                          
        /  /  \         /  /\         ___     
       /  / /\ \       /  / /_       /  /\    
      /  / /  \ \     /  / / /\     /  / /    
     /__/ / \__\ |   /  / / /  \   /__/  \    
     \  \ \ /  / /  /__/ / / /\ \  \__\/\ \__ 
      \  \ \  / /   \  \ \/ / / /     \  \ \/\
       \  \ \/ /     \  \  / / /       \__\  /
        \  \  /       \__\/ / /        /__/ / 
         \__\/          /__/ /         \__\/  
                        \__\/                   v{cli.version()}
    """).strip())
    print()
    cli.startup(args.backend)
    print("\nEnter \"help\" for usage hints.")

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
                    pass # argparse tries to exit on error — suppress that in shell
            else:
                handler(args)
            
        except KeyboardInterrupt:
            cli.exit_cli([])
        except EOFError:
            print()
            cli.exit_cli([])
        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    main()
