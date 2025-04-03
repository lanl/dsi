import shlex
import sys
import os

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dsi.core import Terminal

a = Terminal(debug=0, backup_db = False, runTable=False)

def help_fn(args):
    print("help                                  Shows this help")
    print("load <path to database> [table name]  Loads a database [optional: provide a table name if only that table should be loaded]")
    print("exit                                  Exit the DSI command line Interface")


def version():
    print("DSI version XXX")
    print("Enter \"help\" for usage hints.")


def info(args):
    data = a.artifact_handler(interaction_type='query', query = "SELECT * FROM runTable;")
    print(data)


def load(args):
    db_filename = args[0]
    if len(args) > 1:
        table_name = args[2]

    file_extension = db_filename.rsplit(".", 1)[-1]
    if file_extension == 'db' or file_extension == 'sqlite' or file_extension == 'sqlite3':
        print(db_filename)
        try:
            a.load_module('backend','Sqlite','back-write', filename=db_filename)
            print(f"{db_filename} successfully loaded.")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}")    
    elif file_extension == 'csv':
        try:
            a.load_module('plugin','Csv','reader', filename=db_filename)
            print(f"{db_filename} successfully loaded.")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}")
    elif file_extension == 'toml':
        try:
            a.load_module('plugin','TOML1','reader', filename=db_filename)
            print(f"{db_filename} successfully loaded.")
        except Exception as e:
            print(f"An error {e} occurred loading {db_filename}")
    else:
        print("This database format is not supported. Currently supported formats are: ")
        print("   - sqlite (extension: .db, .sqlite, .sqlite3)")
        print("   - csv (extension: .csv)")
        print("   - toml)")


def exit_shell(args):
    a.close()
    print("Exiting...")
    exit(0)


def modules(args):
    if args[0] == 'avail':
        print(a.list_available_modules())
    elif args[0] == 'list':
        print(a.list_loaded_modules())
    elif args[0] == 'load':
        print(args[1])
        a.load_module(args[1])
    else:
        print(f"Unknown command: {args[0]}")
        print("Type \"help\" to get valid commands")


COMMANDS = {
    'load' : load,
    'info' : info,
    'help': help_fn,
    'module' : modules,
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
                print("Type \"help\" to get valid commands")
        except KeyboardInterrupt:
            print("\nUse 'exit' to leave.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
