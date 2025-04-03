import shlex
import os

from db_utils import *

a = Store()

def help_fn(args):
    print("display <table name>                 Display the contents of that table")
    print("exit                                 Exit the DSI command line Interface")
    print("export <filename>                    Export the contents of DSI database to sqlite")
    print("help                                 Shows this help")
    print("import <filename>                    Import the contents of this file to the current DSI database")
    print("list                                 Lists the tables in the databse")
    print("load <filename>                      Loads this file to a DSI database")
    print("schema <table>                       Get the schema of a specific table")
    print("query <SQL query>                    Queries the database")
    print("query_export <SQL query> <csv_file>  Queries the database and output to CSV\n")
    

def version():
    print("DSI version XXX")
    print("Enter \"help\" for usage hints.\n")


def info(args):
    a.info()


def schema(args):
    try:
        a.get_schema(args[0])
    except Exception as e:
        print(f"An error {e} occurred getting the schema of table {args[0]}\n")   


def load(args):
    db_filename = args[0]
    file_extension = db_filename.rsplit(".", 1)[-1]
    if file_extension == 'db' or file_extension == 'sqlite' or file_extension == 'sqlite3':
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


def import_file(args):
    db_filename = args[0]
    file_extension = db_filename.rsplit(".", 1)[-1]
    
    if file_extension == 'csv':
        try:
            a.load_csv_to_sqlite(args[0])
            a.get_num_tables()
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}\n") 
    else:
        print("This database format is not supported. Currently supported imports formats are: ")
        print("   - csv (extension: .csv)\n")


def export_to_file(args):
    a.copy_sqlite_db(args[0])


def display(args):
    a.pretty_query(f"Select * from {args[0]}")
    print("\n")


def query(args):
    a.pretty_query(args[0])
    print("\n")


def query_export(args):
    a.query_to_csv(args[0], args[1])
    print("\n")


def exit_shell(args):
    print("Exiting...")
    exit(0)



COMMANDS = {
    'display' : display,
    'import' : import_file,
    'export' : export_to_file,
    'list' : info,
    'load' : load,
    'help': help_fn,
    'schema' : schema,
    'query' : query,
    'query_export' : query_export,
    'exit': exit_shell
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
