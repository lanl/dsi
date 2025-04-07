import shlex
import readline  # Enables line editing
import os
import urllib.request
import glob
import ssl
import certifi

from db_utils import *

a = Store()


def path_completer(text, state):
    """Completes file and directory paths for the current input"""
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



def help_fn(args):
    print("display <table name>  [rows]         Display the contents of that table, num rows is optional")
    print("exit                                 Exit the DSI command line Interface")
    print("export_table <table name> <csv_file> Export the contents of that table database to csv")
    print("fetch <database url>                 Fetch and loads a remote databse")
    print("help                                 Shows this help")
    print("import <filename>                    Import the contents of this file to the current DSI database")
    print("list                                 Lists the tables in the databse")
    print("load <filename>                      Loads this file to a DSI database")
    print("query <SQL query> [num rows]         Queries the database, num rows is optional")
    print("query_export <SQL query> <csv_file>  Queries the database and output to CSV")
    print("save <filename>                      Save the local database as filename")
    print("schema <table>                       Get the schema of a specific table\n")
    

    
def clear(args):
    os.system('cls' if os.name == 'nt' else 'clear')


def display(args):
    
    num_rows = 25
    if (len(args) > 1):
        num_rows = args[1]

    results, headers = a.query(f"Select * from {args[0]}")
    a.pretty_print(headers, results, num_rows)
    print("\n")


def exit_shell(args):
    print("Exiting...")
    exit(0)


def export_csv(args):
    results, headers = a.query(f"Select * from {args[0]}")
    a.output_csv(headers, results, args[1])
    print("\n")


def export_to_file(args):
    query = f"select * from {args[0]}"
    a.query_to_csv(query, args[1])
    print("\n")


def fetch(args):
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
        
        load([output_path])
    except Exception as e:
        print(f"Download failed: {e}")
    
         
def import_file(args):
    db_filename = args[0]
    file_extension = db_filename.rsplit(".", 1)[-1]
    
    if file_extension == 'csv':
        try:
            a.load_csv_to_sqlite(args[0])
            a.get_num_tables()
            print("\n")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}\n") 
    else:
        print("This database format is not supported. Currently supported imports formats are: ")
        print("   - csv (extension: .csv)\n")


def info(args):
    db_info = a.info()
    for table in db_info:
        print(f"Table: f{table[0]}")
        print(f"  num of columns: f{table[1]}")
        print(f"  num of rows: f{table[2]}")
    print("\n")
    

def load(args):
    db_filename = args[0]
    file_extension = db_filename.rsplit(".", 1)[-1]
    if a.is_sqlite3_file(db_filename):
        try:
            a.connect_to_db(db_filename)
            a.get_num_tables()
            print(f"{db_filename} successfully loaded.\n")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}\n")    

    elif file_extension == 'csv':
        try:
            if os.path.exists(".temp.db"):
                os.remove(".temp.db")

            a.connect_to_db(".temp.db")
            a.load_csv_to_sqlite(args[0])
            a.get_num_tables()
            print(f"{args[0]} successfully loaded.\n")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}\n")   

    else:
        print("This database format is not supported. Currently supported formats are: ")
        print("   - csv (extension: .csv)")
        print("   - sqlite (extension: .db, .sqlite, .sqlite3)\n")


def query(args):
    num_rows = 25
    if (len(args) > 1):
        num_rows = args[1]

    results, headers = a.query(args[0])
    a.pretty_print(headers, results, num_rows)
    print("\n")


def query_export(args):
    a.query_to_csv(args[0], args[1])
    print("\n")


def save_to_file(args):
    a.copy_sqlite_db(args[0])


def schema(args):
    try:
        headers, rows = a.get_schema(args[0])
        a.pretty_print(headers, rows, -1)
    except Exception as e:
        print(f"An error {e} occurred getting the schema of table {args[0]}\n")   


def version():
    print("DSI version XXX")
    print("Enter \"help\" for usage hints.\n")





COMMANDS = {
    'clear': clear,
    'display' : display,
    'exit': exit_shell,
    'export_table' : export_csv,
    'fetch' : fetch,
    'import' : import_file,
    'help': help_fn,
    'list' : info,
    'load' : load,
    'query' : query,
    'query_export' : query_export,
    'schema' : schema,
    'save' : save_to_file
}


def main():
    version()
    

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
