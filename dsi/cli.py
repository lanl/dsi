import sys
import shlex
import readline 
import glob
import argparse

#from dsi_shim import *
from dsi.core import Terminal
import dsi

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
        return
    
    def startup(self, backend="sqlite3"):
        #self.a = DSI_Shim(backend)
        self.a = Terminal(debug = 2, runTable=True)
        return
    

    def help_fn(self, args):
        print("display <table name> [-n num rows] [-e filename]  Displays the contents of that table, num rows to display is ")
        print("                                                      optional, and it can be exported to a csv/parquet file")
        print("exit                                              Exit the DSI Command Line Interface (CLI)")
        print("find <var>                                        Search for a variable in the dataset")
        print("help                                              Shows this help")
        print("list                                              Lists the tables in the current DSI databse")
        print("load <filename>                                   Loads this filename/url to a DSI database")
        print("query \"<SQL query>\" [-n num rows] [-e filename]   Runs a query, num rows to display is optional")
        print("                                                      , and it can be exported to a csv/parquet file")
        print("save <filename>                                   Save the local database as <filename>")
        print("summary [-t table] [-n num_rows]                  Get a summary of the database or just a table and optionally ")
        print("                                                     specify number of data rows to display\n")
    
    
    def clear(self, args):
        '''
        Clears the command line
        '''
        os.system('cls' if os.name == 'nt' else 'clear')




    def get_display_parser(self):
        parser = argparse.ArgumentParser(prog='display')
        parser.add_argument('table_name', help='Table to display')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows  for each table')
        parser.add_argument('-e', '--export', type=str, required=False, default="", help='Export to csv or parquet file')
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

        #self.a.display_table(table_name, num_rows)
        self.a.display(table_name, num_rows)
        
        
        if args.export != None:
            if args.export == "":
                filename = args[0] + ".csv"
            else:
                filename = args.export
            self.a.export_table(table_name, filename)


    def exit_cli(self, args):
        '''
        Exits the CLI
        '''
        print("Exiting...")
        self.a.close()
        exit(0)


    def export_table(self, args):
        '''
        Exports to a csv/parquet file
        '''
        self.a.export_table(args[0], args[1])
        
        
    def find(self, args):
        '''
        Global find to see where that string exists
        '''
        self.a.find(args[0])
              
              
    def list_tables(self, args):
        '''
        Lists the tables in the database
        '''
        #self.a.list_tables()
        self.a.list()
    
    
    def load(self, args):
        '''
        Loads data to the DSI database
        '''
        table_name = ""
        if len(args) > 1:
            table_name = args[1]
        #self.a.load(args[0], table_name)



        
       
   
    def get_query_parser(self):
        parser = argparse.ArgumentParser(prog='display')
        parser.add_argument('sql_query', help='SQL query to execute')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows  for each table')
        parser.add_argument('-e', '--export', type=str, required=False, default="", help='Export to csv or parquet file')
        return parser

    def query(self, args):
        '''
        Runs the query sent to it
        '''
        sql_query = args.sql_query
        num_rows = 25
        if args.num_rows != None:
            num_rows = args.num_rows

        self.a.query(sql_query, num_rows)
        
        
        if args.export != None:
            if args.export == "":
                filename = args[0] + ".csv"
            else:
                filename = args.export
            self.a.export_query(sql_query, filename)
        
        

    def save_to_file(self, args):
        '''
        Save the database to file
        '''
        self.a.save_to_file(args[0])



    def get_summary_parser(self):
        parser = argparse.ArgumentParser(prog='summary')
        parser.add_argument('-t', '--table', type=str, required=False, help='Show only this table')
        parser.add_argument('-n', '--num_rows', type=int, required=False, help='Show n rows for each table')
        return parser
    
    def summary(self, args):
        '''
        Get the summary of a table or database
        '''
        
        table_name = ""
        if args.table != None:
            table_name = args.table
           
        num_rows = 0
        if args.num_rows != None:
            num_rows = args.num_rows
        
        #self.a.summarize(table_name, num_rows)
        self.a.summary(table_name, num_rows)



    def version(self):
        '''
        Output the version of DSI being used
        '''
        print("DSI version " + str(dsi.__version__)+"\n")
        print("Enter \"help\" for usage hints.\n")


cli = DSI_Cli()


COMMANDS = {
    'clear': (None, cli.clear), #
    'display' : (cli.get_display_parser, cli.display), #
    'exit': (None, cli.exit_cli), #
    'find' : (None, cli.find),
    'help': (None, cli.help_fn), #
    'list' : (None, cli.list_tables), #
    'load' : (None, cli.load),
    'query' : (cli.get_query_parser, cli.query),
    'save' : (None, cli.save_to_file),
    'summary' : (cli.get_summary_parser, cli.summary), #
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--backend", type=str, default="sqlite", help="Supported backends are sqlite and duckdb")

    cli.version()
    
    while True:
        try:
            cli.a.startup(args.backend)
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
