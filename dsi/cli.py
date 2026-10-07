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
import io
import json
import asyncio
import subprocess
import importlib.util
import getpass
import socket
import yaml
from pathlib import Path
from datetime import datetime

from dsi.core import Terminal
from dsi.sync import Sync
from dsi.utils.data_acquisition import discover_endpoints_async, download_csv_files_async, pull_data
from dsi.utils.acquisition.utils import create_directory, combine_csv, create_hashed_folder_from_path, split_path, upsert_records

from ._version import __version__

def autofill_path(text, state):
    """Completes CLI actions as first token, then file/directory paths."""
    line = readline.get_line_buffer()
    endidx = readline.get_endidx()

    try:
        parts = shlex.split(line[:endidx], posix=True)
    except ValueError:
        parts = line[:endidx].split()

    if line[:endidx] and line[:endidx][-1].isspace():
        parts.append('')

    curr = parts[-1] if parts else ''

    if len(parts) <= 1:
        matches = [a for a in COMMANDS.keys() if a.startswith(curr)]
        matches += path_matches(curr)
    else:
        matches = path_matches(curr)

    try:
        return matches[state]
    except IndexError:
        return None


def path_matches(curr):
    matches = glob.glob(os.path.expanduser(curr) + "*")
    matches = [m + "/" if os.path.isdir(m) else m for m in matches]
    matches = [m[m[:-1].rfind("/") + 1:] if "/" in m[:-1] else m for m in matches]
    return matches


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


    def viewers_check(self):
        '''
        Checks which viewers are available depending on the user's environment.

        To register a new viewer, add it to all_viewers with the viewer name as the key and its required PyPI packages as the value.
        Also include a checker to test if those packages can be installed (using import name. ex: sklearn for the scikit-learn)
        '''
        # both objects must be include only lowercase strings
        available_viewers = []
        all_viewers = {"dashboard" : ["streamlit"], 
                       "ml" : ["streamlit", "scikit-learn"] #pypi package names
                      }

        # dashboard checker
        if importlib.util.find_spec("streamlit") is not None:
            available_viewers.append("dashboard")

        # ml checker
        if all(importlib.util.find_spec(pkg) is not None for pkg in ["streamlit", "sklearn"]):
            available_viewers.append("ml")

        # # jupyter checker
        # if all(importlib.util.find_spec(pkg) is not None for pkg in ["ipykernel", "nbformat"]):
        #     available_viewers.append("jupyter")

        return (None, all_viewers) if not available_viewers else (available_viewers, all_viewers)


    def startup(self, backend="sqlite"):
        self.t = Terminal(debug = 0, runTable=False)
        self.t.user_wrapper = True

        self.valid_viewers, self.all_viewers = self.viewers_check()
        self.start_dir = os.getcwd()
        self.db_path = os.path.join(self.start_dir, ".temp_dsi.db")

        fnull = open(os.devnull, 'w')
        if not self.t.can_create_file_here(self.start_dir):
            print("Cannot start the DSI CLI due to write permissions in this directory. Please try elsewhere.")
            with redirect_stdout(fnull):
                self.exit_cli([])

        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        keywords = None
        if backend.lower() == "ndp":
            keywords = input("Enter NDP search keywords (e.g., climate, temperature): ").strip()
            
        try:
            with redirect_stdout(fnull):
                if backend=="duckdb":
                    self.t.load_module('backend','DuckDB','back-write', filename = self.db_path)
                    self.name = "duckdb"
                    backend = "DuckDB"
                elif backend.lower() == "ndp":
                    # NDP requires params dict for initialization
                    self.t.load_module('backend', 'NDP', 'back-read', params={"keywords": keywords})
                    self.name = "ndp"
                    backend = "NDP"
                elif backend.lower() == "osti":
                    # PLACEHOLDER for future implement
                    # OSTI requires params dict for initialization
                    print("OSTI backend requires configuration.")
                    print("Use Python API: t.load_module('backend', 'OSTI', 'back-read', params={...})")
                    backend = "OSTI"
                    self.exit_cli([])
                else:
                    backend = "Sqlite"
                    self.t.load_module('backend','Sqlite','back-write', filename = self.db_path)
                    self.name = "sqlite"
        except Exception as e:
            print(f"backend ERROR: {e}")
            with redirect_stdout(fnull):
                self.exit_cli([])
        print(f"Created a temporary {backend} DSI backend")


    def help_fn(self, args):
        commands = {
            'cd' : ("<path>", "Changes the working directory within the CLI environment."),
            'display' :("<table name> [-n num_rows] [-e filename]",
                        "Displays a table's data. Optionally limit displayed rows and export to CSV/Parquet"),
            'draw' :("[-f filename]", "Draws an ER diagram of all tables in the current DSI database"),
            'exit': ("", "Exits the DSI Command Line Interface (CLI)"),
            'find' : ("<condition>", "Finds all rows of a table that match a column-level condition."),
            'help': ("", "Shows this help message. For help with a command, enter <command name> -h"),
            'list' : ("", "Lists all tables in the current DSI database"),
            'ls' : ("", "Lists all files in the current or specified directory."),
            'ls_endpoints' : ("<hpc_name> ... --script_path <path>",
                              "Discovers remote DSI endpoints on one or more HPC systems and saves them to a session file."),
            'ls_databases' : ("[--session session_file]",
                              "Pulls the source catalogs from endpoints discovered by ls_endpoints and lists the databases found."),
            'pull_data' : ("<location_type> <remote_location> <remote_path> [-d download_location]",
                                      "Directly calls DSI's pull_data() to download a single file from a source. Enter 'pull_data -h' to learn the inputs."),
            'pull_db' : ("<database_name> ... | --all [-w workspace_folder]",
                        "Downloads one or more databases listed by ls_databases to a workspace folder."),
            'plot_table' : ("<table name> [-f filename]", "Plots numerical data from a table to an optional file name argument"),
            'query' : ("<SQL query> [-n num_rows] [-e filename]",
                       "Executes a SQL query (in quotes). Optionally limit printed rows or export to CSV/Parquet"),
            'read' : ("<data source> [-t table_name]", "Reads a file or URL into the DSI database. Optionally set table name."),
            'search' : ("<value>", "Searches for a string or number across DSI."),
            'summary' : ("[-t table_name]", "Summary of the database or a specific table."),
            'viewers' : ("", "Prints the available viewers for the user."),
            'view' : ("<available viewer>", "Creates an instance of the DSI viewer in another application."),
            'write' : ("<filename>", "Writes data in DSI database to a permanent location."),
        }
        if self.viewers_check()[0] is None:
            del commands["view"]

        print()
        terminal_width = shutil.get_terminal_size().columns
        for key, val in commands.items():
            cmd = f'{key} {val[0]}'
            wrapped = textwrap.fill(f"{cmd:48} {val[1]}", width=terminal_width, subsequent_indent=' ' * 50)
            print(wrapped)
            if '\n' in wrapped: print()
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
        if args.num_rows is not None:
            num_rows = args.num_rows

        try:
            self.t.display(table_name, num_rows)
        except Exception as e:
            print(f"display ERROR: {e}")
            return

        if args.export is not None:
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
        if args.filename is not None:
            erd_name = args.filename

        error = self.export_table("dsi_erd_gen", erd_name)
        if error != 1:
            print(f"Saved an ER Diagram at {erd_name}")
        print()


    def exit_cli(self, args):
        '''
        Exits the CLI
        '''
        print("\nExiting...")
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
                    self.t.load_module('plugin', "Parquet_Writer", "writer", filename = filename, table_name = table_name)
                else:
                    success_load = False
        except Exception as e:
            self.t.active_metadata = OrderedDict()
            print(f"export ERROR: {e}")
            return 1

        if success_load:
            try:
                self.t.transload()
            except Exception as e:
                self.t.active_modules['writer'].pop(0)
                self.t.active_metadata = OrderedDict()
                print(f"export ERROR: {e}")
                return 1

        self.t.active_metadata = OrderedDict()


    def get_federate_parser(self):
        parser = argparse.ArgumentParser(prog='federate')
        parser.add_argument('config_file', help='YAML config file that lists all data sources to download')
        parser.add_argument('-w', '--workspace_folder', type=str, required=False, default="", help='folder to store all downloaded sources')
        return parser

        

    def find(self, args):
        '''
        Global find to see where the condition is met
        '''
        if not args:
            print("find ERROR: need to specify an object to find")
            return
        if len(args) > 1:
            print("find ERROR: need to wrap a multi-word search in quotes. Ex: find 'this is'")
            return
        query = args[0]

        if not isinstance(query, str):
            print("find ERROR: <condition> must be a string.")
            return
        operators = ['==', '!=', '>=', '<=', '=', '<', '>', '(', "~", "~~"]
        if not any(op in query for op in operators):
            print("find ERROR: <condition> must contain an operator. If just searching for a value, use the 'search' command.")
            return

        print(f"Finding all rows where '{query}'")
        output = None
        try:
            f = io.StringIO()
            with redirect_stdout(f):
                find_data = self.t.find_relation(query)
            output = f.getvalue()
        except Exception as e:
            e = str(e).replace("query_object", "condition")
            print(f"find ERROR: {e}")
            return

        if output and "WARNING" in output:
            warn_msg = output[output.find("WARNING"):]
            if "artifact_handler" in warn_msg:
                lines = warn_msg.splitlines()
                start = lines[1].find('`')
                between = lines[1][start + 1 : lines[1].find('`', start + 1)]
                lines[1] = lines[1].replace(between, "query")
                lines[2] = lines[2].replace(lines[2][lines[2].find('artifact'):-1], "`query`")
                warn_msg = '\n'.join(lines)
            elif "Could not find" in warn_msg:
                ending_ind = warn_msg.find("in this database")
                warn_msg = warn_msg[:40] + query + warn_msg[ending_ind-2:]
            print("\n"+warn_msg.replace("database", "backend"))
            return

        table_name = None
        output_df = None
        row_list = [f.row_num for f in find_data]
        for val in find_data:
            if table_name is None:
                table_name = val.t_name
                output_df = pd.DataFrame([val.value], columns=val.c_name)
            else:
                output_df.loc[len(output_df)] = val.value

        print(f'\nTable: {table_name}')
        output_df.insert(0, "row_index", row_list)
        self.t.table_print_helper(output_df.columns.tolist(), output_df.values.tolist(), output_df.shape[0])
        print()


    def get_data_parser(self):
        parser = argparse.ArgumentParser(prog='get_data')
        parser.add_argument('database_name', help='Previously downloaded database whose data will be downloaded')
        parser.add_argument('-w', '--workspace_folder', type=str, required=False, default="", help='folder to store all downloaded data')
        return parser


    def get_data(self, args):
        db_name = args.database_name

        workspace_folder = None
        if args.workspace_folder is not None and args.workspace_folder != "":
            workspace_folder = args.workspace_folder
        
        try:
            curr_tables = self.t.list(True)
        except Exception:
            print("get_data ERROR: Call 'federate' first to access data associated with a downloaded database.")
            return
        
        if "federation" not in curr_tables:
            print("get_data ERROR: Call 'federate' first to access data associated with a downloaded database.")
            return
        
        try:
            s = Sync(self.db_path)
            s.get_data(db_name, workspace_folder)
        except Exception as e:
            print(f"get_data ERROR: {e}")
            return


    def get_ls_endpoints_parser(self):
        parser = argparse.ArgumentParser(prog='ls_endpoints')
        parser.add_argument('hpc_names', nargs='+', help='One or more HPC hostnames to discover endpoints on')
        parser.add_argument('--script_path', required=True, help='Path to load_dsi_endpoints.sh on the remote host')
        parser.add_argument('--prefix', dest='prefixes', action='append',
                             help='Endpoint env var prefix to look for (repeatable). Default: DSI_ENDPOINT_, DIANA_ENDPOINT_')
        parser.add_argument('--username', type=str, required=False, default="", help='Username on the HPC system(s)')
        parser.add_argument('--kerberos', action='store_true', help='Use Kerberos authentication via a jump host')
        parser.add_argument('--jump_host', type=str, required=False, help='Jump host hostname (required with --kerberos)')
        parser.add_argument('--jump_username', type=str, required=False, help='Username on the jump host')
        parser.add_argument('--reticket_cmd', type=str, required=False, default='reticket',
                             help='Kerberos ticket-init command to run on the jump host (default: reticket)')
        parser.add_argument('--session', type=str, required=False, default="",
                             help='Session file to write discovered endpoints to (default: .dsi_endpoints_<timestamp>.json)')
        return parser


    def ls_endpoints(self, args):
        '''
        Discovers DSI endpoints on one or more remote HPC systems and saves them to a session file
        for use by ls_databases.
        '''
        if args.kerberos and not args.jump_host:
            print("ls_endpoints ERROR: --jump_host is required when using --kerberos")
            return

        username = args.username
        if not username:
            username = input("Enter the username for the HPC system(s): ")

        password = getpass.getpass("Password (leave blank to use SSH keys): ")
        password = password if password else None

        jump_username = args.jump_username
        jump_password = None
        if args.kerberos:
            if not jump_username:
                jump_username = input(f"Enter the username for jump host {args.jump_host}: ")
            jump_password = getpass.getpass(f"Password for jump host {args.jump_host}: ")
            jump_password = jump_password if jump_password else None

        prefixes = args.prefixes if args.prefixes else ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']
        hpc_type = 'kerberos' if args.kerberos else 'standard'

        all_endpoints = {}
        jump_host_required = {}
        failed = []

        for hpc_name in args.hpc_names:
            print(f"Discovering endpoints on {hpc_name}...")
            try:
                endpoints = asyncio.run(discover_endpoints_async(
                    hostname=hpc_name,
                    username=username,
                    script_path=args.script_path,
                    prefixes=prefixes,
                    password=password,
                    hpc_type=hpc_type,
                    jump_host=args.jump_host,
                    jump_username=jump_username,
                    jump_password=jump_password,
                    reticket_cmd=args.reticket_cmd
                ))
            except Exception as e:
                print(f"ls_endpoints ERROR: Failed to discover endpoints on {hpc_name}: {e}")
                failed.append(hpc_name)
                continue

            if not endpoints:
                print(f"  No endpoints found on {hpc_name}")
                failed.append(hpc_name)
                continue

            for endpoint_name, endpoint_path in endpoints.items():
                all_endpoints[f"{hpc_name}::{endpoint_name}"] = endpoint_path
            if args.kerberos:
                jump_host_required[hpc_name] = True
            print(f"  Found {len(endpoints)} endpoint(s) on {hpc_name}")

        if not all_endpoints:
            print(f"\nNo endpoints found on any host. Failed: {', '.join(failed)}\n")
            return

        session_file = args.session
        if not session_file:
            session_file = f".dsi_endpoints_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        session_data = {
            'endpoints': all_endpoints,
            'username': username,
            'hpc_type': hpc_type,
            'jump_host': args.jump_host,
            'jump_username': jump_username,
            'jump_host_required': jump_host_required,
            'reticket_cmd': args.reticket_cmd,
        }
        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)

        print(f"\nDiscovered {len(all_endpoints)} endpoint(s):")
        for name, path in all_endpoints.items():
            print(f"  {name}: {path}")
        print(f"\nSaved session to {session_file}")
        print(f"Run 'ls_databases --session {session_file}' to see available databases.\n")


    def get_ls_databases_parser(self):
        parser = argparse.ArgumentParser(prog='ls_databases')
        parser.add_argument('--session', type=str, required=False, default="",
                             help='Session file produced by ls_endpoints (default: most recently modified .dsi_endpoints_*.json)')
        return parser


    def ls_databases(self, args):
        '''
        Pulls the source catalog (CSV) from each endpoint discovered by ls_endpoints,
        and lists the databases they reference.
        '''
        session_file = args.session
        if not session_file:
            candidates = sorted(glob.glob(".dsi_endpoints_*.json"), key=os.path.getmtime, reverse=True)
            if not candidates:
                print("ls_databases ERROR: No endpoint session found. Run 'ls_endpoints' first.")
                return
            session_file = candidates[0]

        if not os.path.exists(session_file):
            print(f"ls_databases ERROR: Session file not found: {session_file}")
            return

        with open(session_file, 'r') as f:
            session_data = json.load(f)

        endpoints = session_data.get('endpoints', {})
        if not endpoints:
            print("ls_databases ERROR: No endpoints in session file.")
            return

        username = session_data.get('username', '')
        jump_host = session_data.get('jump_host')
        jump_username = session_data.get('jump_username')
        reticket_cmd = session_data.get('reticket_cmd', 'reticket')
        jump_host_required_map = session_data.get('jump_host_required', {})

        password = getpass.getpass(f"Password for {username} (leave blank to use SSH keys): ")
        password = password if password else None

        jump_password = None
        if jump_host:
            jump_password = getpass.getpass(f"Password for jump host {jump_host}: ")
            jump_password = jump_password if jump_password else None

        # Group endpoints by cluster
        cluster_endpoints = {}
        for endpoint_name, endpoint_path in endpoints.items():
            cluster_name, original_endpoint = endpoint_name.split('::', 1)
            cluster_endpoints.setdefault(cluster_name, {})[original_endpoint] = endpoint_path

        temp_dir = f".ls_databases_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        create_directory(dir_name=temp_dir, delete_if_exists=True)

        all_databases = []
        try:
            for cluster_name, csv_paths in cluster_endpoints.items():
                print(f"Pulling catalog from {cluster_name}...")
                jump_required = jump_host_required_map.get(cluster_name, False)
                try:
                    downloaded_files = asyncio.run(download_csv_files_async(
                        hostname=cluster_name,
                        csv_paths=csv_paths,
                        temp_folder=temp_dir,
                        username=username,
                        password=password,
                        jump_host=jump_host,
                        jump_username=jump_username,
                        jump_password=jump_password,
                        jump_host_required=jump_required,
                        reticket_cmd=reticket_cmd
                    ))
                except Exception as e:
                    print(f"  ls_databases ERROR: Failed to pull catalog from {cluster_name}: {e}")
                    continue

                if not downloaded_files:
                    print(f"  No catalog files downloaded from {cluster_name}")
                    continue

                output_csv = str(Path(temp_dir) / f"output_{cluster_name}.csv")
                try:
                    rows = combine_csv(temp_dir, output_csv)
                except Exception as e:
                    print(f"  ls_databases ERROR: Failed to parse catalog from {cluster_name}: {e}")
                    continue

                for row in rows:
                    row['cluster'] = cluster_name
                all_databases.extend(rows)

            if not all_databases:
                print("\nNo databases found.\n")
                return

            databases_session_file = f".dsi_databases_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(databases_session_file, 'w') as f:
                json.dump(all_databases, f, indent=2)

            headers = ['cluster', 'location_type', 'location', 'path', 'type', 'submitter_name']
            rows = [[db.get(h, '') for h in headers] for db in all_databases]
            self.t.table_print_helper(headers, rows, len(rows))

            print(f"\nFound {len(all_databases)} database(s) across {len(cluster_endpoints)} cluster(s).")
            print(f"Saved to {databases_session_file}")
            print(f"Run 'pull_db' to download one or more of these databases.\n")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


    def get_pull_db_parser(self):
        parser = argparse.ArgumentParser(prog='pull_db')
        parser.add_argument('database_name', nargs='*',
                             help='Name(s) of database file(s) to download, as listed by ls_databases. Omit with --all to download every database.')
        parser.add_argument('--all', action='store_true', help='Download every database listed in the session')
        parser.add_argument('--session', type=str, required=False, default="",
                             help='Databases session file produced by ls_databases (default: most recently modified .dsi_databases_*.json)')
        parser.add_argument('-w', '--workspace_folder', type=str, required=False, default="",
                             help='Folder to store downloaded databases (default: current directory)')
        return parser


    def pull_db(self, args):
        '''
        Downloads one or more databases listed by ls_databases to a workspace folder.
        '''
        if not args.database_name and not args.all:
            print("pull_db ERROR: Specify one or more database names, or use --all.")
            return

        session_file = args.session
        if not session_file:
            candidates = sorted(glob.glob(".dsi_databases_*.json"), key=os.path.getmtime, reverse=True)
            if not candidates:
                print("pull_db ERROR: No databases session found. Run 'ls_databases' first.")
                return
            session_file = candidates[0]

        if not os.path.exists(session_file):
            print(f"pull_db ERROR: Session file not found: {session_file}")
            return

        with open(session_file, 'r') as f:
            all_databases = json.load(f)

        if not all_databases:
            print("pull_db ERROR: No databases in session file.")
            return

        if args.all:
            selected = all_databases
        else:
            by_name = {}
            for db in all_databases:
                by_name.setdefault(os.path.basename(db.get('path', '')), []).append(db)

            selected = []
            for name in args.database_name:
                matches = by_name.get(name)
                if not matches:
                    print(f"pull_db ERROR: No database named '{name}' found in {session_file}. Run 'ls_databases' to see available databases.")
                    return
                selected.extend(matches)

        workspace_folder = args.workspace_folder if args.workspace_folder else os.getcwd()
        workspace_path = str(Path(workspace_folder).resolve())
        create_directory(dir_name=workspace_path)

        # Cache HPC usernames/passwords per-location so multi-database pulls only prompt once per location
        creds_by_location = {}
        success_count = 0

        for db in selected:
            location_type = db.get('location_type', '').strip().lower()
            location = db.get('location')
            remote_path = db.get('path')
            cluster = db.get('cluster', location)

            print(f"\nDownloading {location}:{remote_path}")

            username = ""
            password = None
            if location_type == 'hpc':
                if location not in creds_by_location:
                    entered_username = input(f"Enter the username for {location}: ")
                    entered_password = getpass.getpass(f"Password for {entered_username}@{location} (leave blank to use SSH keys): ")
                    creds_by_location[location] = (entered_username, entered_password if entered_password else None)
                username, password = creds_by_location[location]

            try:
                folder_hash, download_folder = create_hashed_folder_from_path(remote_path, workspace_path)
                downloaded_file_path = pull_data(
                    location_type=location_type,
                    remote_location=location,
                    remote_path=remote_path,
                    download_location=download_folder,
                    username=username,
                    password=password
                )
            except Exception as e:
                print(f"  pull_db ERROR: Failed to download {location}:{remote_path}: {e}")
                continue

            if not downloaded_file_path:
                print(f"  pull_db ERROR: Download failed for {location}:{remote_path}")
                continue

            local_folder, local_filename = split_path(downloaded_file_path)
            db_info = {
                "original_location_type": location_type,
                "original_location": location,
                "original_path": remote_path,
                "folder_hash": folder_hash,
                "local_path": local_folder,
                "name": local_filename,
                "source_cluster": cluster,
            }
            upsert_records(f"{workspace_path}/dsi_database_list.json", [db_info], key="original_path")

            print(f"  Downloaded to {downloaded_file_path}")
            success_count += 1

        print(f"\nDownloaded {success_count} of {len(selected)} database(s) to {workspace_path}.")
        print(f"Use 'read {workspace_path}/dsi_database_list.json' or 'federate' to load them into DSI.\n")


    def get_pull_data_parser(self):
        parser = argparse.ArgumentParser(prog='pull_data')
        parser.add_argument('location_type', help='Type of the source location (e.g. "hpc", "url", "github", "s3", "local")')
        parser.add_argument('remote_location', help='Location of the data (e.g. hostname for HPC, URL for web, bucket for S3)')
        parser.add_argument('remote_path', help='Path to the data at the remote location')
        parser.add_argument('-d', '--download_location', type=str, required=False, default="",
                             help='Folder to download the file to (default: current directory)')
        parser.add_argument('-u', '--username', type=str, required=False, default="", help='Username, for HPC access')
        parser.add_argument('--download_limit', type=int, required=False, default=10485760,
                             help='Max file size in bytes to download without confirmation; 0 for no limit (default: 10485760)')
        parser.add_argument('--jump_host', type=str, required=False, help='Jump host hostname, for Kerberos authentication')
        parser.add_argument('--jump_username', type=str, required=False, help='Username on the jump host')
        parser.add_argument('--jump_host_required', action='store_true', help='Whether the jump host is required to reach remote_location')
        parser.add_argument('--reticket_cmd', type=str, required=False, default='reticket',
                             help='Kerberos ticket-init command to run on the jump host (default: reticket)')
        return parser


    def pull_data_fn(self, args):
        '''
        Thin wrapper around dsi.utils.data_acquisition.pull_data: downloads a single file
        from a source (hpc, url, github, s3, local) using the given arguments directly.
        '''
        download_location = args.download_location if args.download_location else os.getcwd()
        create_directory(dir_name=download_location)

        password = None
        if args.location_type.strip().lower() == 'hpc':
            password = getpass.getpass(f"Password for {args.username or 'user'}@{args.remote_location} (leave blank to use SSH keys): ")
            password = password if password else None

        jump_password = None
        if args.jump_host:
            jump_password = getpass.getpass(f"Password for jump host {args.jump_host}: ")
            jump_password = jump_password if jump_password else None

        try:
            downloaded_file_path = pull_data(
                location_type=args.location_type,
                remote_location=args.remote_location,
                remote_path=args.remote_path,
                download_location=download_location,
                username=args.username,
                password=password,
                download_limit=args.download_limit,
                jump_host=args.jump_host,
                jump_username=args.jump_username,
                jump_password=jump_password,
                jump_host_required=args.jump_host_required,
                reticket_cmd=args.reticket_cmd
            )
        except Exception as e:
            print(f"pull_data ERROR: {e}")
            return

        if downloaded_file_path:
            print(f"\nDownloaded to {downloaded_file_path}\n")
        else:
            print("\npull_data ERROR: Download failed.\n")


    def list_tables(self, args):
        '''
        Lists the tables in the database
        '''
        try:
            self.t.list()
        except Exception as e:
            if "needs to have data" in str(e).lower():
                print("Currently 0 tables loaded")
            else:
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
            print(f"Saved a plot of the {table_name} table in {filename}")
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
        if args.num_rows is not None:
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
        self.t.table_print_helper(headers, rows, len(rows), num_rows)

        if args.export is not None:
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
        parser.add_argument('data_source', help='Data to read into DSI. Either a filename or a URL to the data')
        parser.add_argument('-t', '--table_name', type=str, required=False, default="", help='table name to store data into')
        return parser


    def read(self, args):
        '''
        Reads data file or a database into DSI

        Args:
            data_source (obj): name of the data source (file, database, URL, etc) to read
            table_name (str): name of the table to store the data within
        '''
        table_name = ""
        if args.table_name != "":
            table_name = args.table_name
        else:
            table_name = os.path.splitext(os.path.basename(args.data_source))[0]

        dbfile = args.data_source
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
                backend_name = self.t.identify_backend(dbfile)
                if backend_name:
                    self.t.load_module('backend',backend_name,'back-read', filename=dbfile)
                    self.t.artifact_handler(interaction_type="process")
                    self.t.unload_module('backend',backend_name,'back-read')
                elif file_extension.lower() == 'csv':
                    self.t.load_module('plugin', "Csv", "reader", filenames = dbfile, table_name = table_name)
                elif file_extension.lower() == 'toml':
                    self.t.load_module('plugin', "TOML", "reader", filenames = dbfile, table_name = table_name)
                elif file_extension.lower() in ['yaml', 'yml']:
                    self.t.load_module('plugin', "YAML", "reader", filenames = dbfile, table_name = table_name)
                elif file_extension.lower() == 'json':
                    self.t.load_module('plugin', "JSON", "reader", filenames = dbfile, table_name = table_name)
                elif file_extension.lower() in ['pq', 'parquet']:
                    self.t.load_module('plugin', "Parquet", "reader", filenames = dbfile, table_name = table_name)
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
                print(f"Loaded {dbfile} into the tables: {', '.join(table_keys)}")
            else:
                print(f"Loaded {dbfile} into the table {table_keys[0]}")

            self.t.num_tables()
            print()
            self.t.active_metadata = OrderedDict()
        else:
            print()
            print("Ensure input data is structured properly.")
            print("Supported formats: ")
            print("   - csv (extension: .csv)")
            print("   - json (extension: .json)")
            print("   - toml (extension: .toml)")
            print("   - yaml (extension: .yaml, .yml)")
            print("   - parquet (extension: .pq, .parquet)")
            print("   - sqlite (extension: .db, .sqlite, .sqlite3)")
            print("   - duckdb (extension: .duckdb, .db)\n")
            return


    def search(self, args):
        '''
        Global search to see where that value exists
        '''
        if not args:
            print("search ERROR: need to specify an object to search for")
            return
        if len(args) > 1:
            print("search ERROR: need to wrap a multi-word search in quotes. Ex: find 'this is'")
            return
        query = args[0]

        val = f"'{query}'" if isinstance(query, str) else query
        print(f"Searching for all instances of {val} in the active backend")

        fnull = open(os.devnull, 'w')
        try:
            with redirect_stdout(fnull):
                find_data = self.t.find_cell(query, row=True)
        except Exception as e:
            print(f"search ERROR: {e}")
            return

        if find_data is None:
            print(f"WARNING: {val} was not found in this backend\n")
            return
        print()
        for val in find_data:
            print(f"Table: {val.t_name}")
            print(f"  - Columns: {val.c_name}")
            print(f"  - Row Number: {val.row_num}")
            print(f"  - Data: {val.value}")
        print()


    def get_summary_parser(self):
        parser = argparse.ArgumentParser(prog='summary')
        parser.add_argument('-t', '--table', type=str, required=False, help='Show only this table')
        return parser


    def summary(self, args):
        '''
        Get the summary of a table or database
        '''
        table_name = None
        if args.table is not None:
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


    def viewers(self, args):
        if self.valid_viewers is None:
            print("There are no available viewers. Install requirements.heavy.txt to access them.")
            for v, p in self.all_viewers.items():
                print(f"  {v:<12}: pip install {' '.join(p)}")
            print()
            return
        print(f'Available viewers are: {", ".join(self.valid_viewers)}\n')


    def view(self, args):
        if not args:
            if self.valid_viewers is not None:
                print(f"view ERROR: need to specify a DSI viewer to use. Available: {', '.join(self.valid_viewers)}")
            else:
                print("view ERROR: need to specify a DSI viewer to use")
            return
        
        viewer = str(args[0]).lower()
        if viewer not in self.all_viewers.keys():
            print("view ERROR: This viewer does not exist.")
            return
        if len(set(args) & set(self.all_viewers.keys())) > 1:
            print("view ERROR: can only use one viewer at a time")
            return
        if viewer not in self.valid_viewers:
            print(f"view ERROR: To load the {viewer} viewer, pip install {' '.join(self.all_viewers[viewer])}")
            return

        if viewer == "dashboard":
            # user must specify at least one directory
            if len(args) == 1:
                print("view ERROR: dashboard viewer requires at least one input directory.")
                return
            # check if other inputs are valid folder paths
            for f in args[1:]:
                if not os.path.isdir(f):
                    print(f"view ERROR: dashboard viewer has an invalid input directory: {f}")
                    return

            PORT = 8501
            dashboard_code_filepath = os.path.join(os.path.dirname(__file__), "plugins", "dashboard.py")
            input_dirs = args[1:]
            self.launch_streamlit_viewer(PORT, "Dashboard", dashboard_code_filepath, extra_args=input_dirs)

        elif viewer == "ml":
            #check if current db is empty
            if not self.t.valid_backend(self.t.loaded_backends[0]):
                print("view ERROR: the ML viewer requires data to run models.")
                return

            PORT = 8502
            ml_code_filepath = os.path.join(os.path.dirname(__file__), "plugins", "ml_emulator.py")
            extra_args = [self.db_path, self.name]
            self.launch_streamlit_viewer(PORT, "ML Emulator", ml_code_filepath, extra_args=extra_args)
            
        else:
            print("view ERROR: input viewer was invalid. Please try again.")
            return


    def launch_streamlit_viewer(self, port: int, app_name: str, app_file: str, extra_args: list):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        if os.name != "nt":
            subprocess.run(f"lsof -t -i:{port} | xargs -r kill -9", shell=True, check=False, 
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.run(f'for /f "tokens=5" %a in (\'netstat -ano ^| findstr :{port}\') do taskkill /F /PID %a >nul 2>&1', 
                        shell=True, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        is_remote = any(os.environ.get(x) for x in ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"])
        if is_remote:
            remote_user = getpass.getuser()
            remote_host = socket.getfqdn() or socket.gethostname()

            print("In a separate terminal on your local machine, run:")
            print(f" ssh -L {port}:localhost:{port} {remote_user}@{remote_host}")
            print(f" (Leave the new terminal running while using the {app_name})")

        print(f"\nView the {app_name} at \033[1;34mhttp://localhost:{port}\033[0m")
        print("To exit, enter [Ctrl+C] here")

        cmd = [sys.executable, "-m", "streamlit", "run", app_file, f"--server.port={port}", "--server.headless=true", 
                   "--browser.gatherUsageStats=false", "--"] + extra_args
        proc = subprocess.Popen(cmd, env=env, stdin=None, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
        try:
            for line in proc.stdout:
                line = line.rstrip("\n")
                if (
                    "You can now view your Streamlit app" in line
                    or "Local URL:" in line
                    or "Network URL:" in line
                    or "External URL:" in line
                    or "For better performance" in line
                    or "$ xcode-select" in line
                    or "$ pip install watchdog" in line
                    or line == ""
                ):
                    continue
                print(line, flush=True)
            proc.wait()
        except KeyboardInterrupt:
            print(f"\n Closing {app_name}.\n")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


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
        dsi_db_path = os.path.join(self.start_dir, ".temp_dsi.db")
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
        print(f"Successfully wrote all data to {final_name}\n")
    

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
    'ls_endpoints' : (cli.get_ls_endpoints_parser, cli.ls_endpoints),
    'ls_databases' : (cli.get_ls_databases_parser, cli.ls_databases),
    'pull_db' : (cli.get_pull_db_parser, cli.pull_db),
    'pull_data' : (cli.get_pull_data_parser, cli.pull_data_fn),
    'plot_table' : (cli.get_plot_table_parser, cli.plot_table),
    'query' : (cli.get_query_parser, cli.query),
    'read' : (cli.get_read_parser, cli.read),
    'search' : (None, cli.search),
    'summary' : (cli.get_summary_parser, cli.summary),
    'viewers' : (None, cli.viewers),
    'view' : (None, cli.view),
    'write' : (cli.get_write_parser, cli.write_to_file),
    'ls' : (None, cli.ls),
    'cd' : (None, cli.cd)
}

def main():
    if sys.argv[1:] and sys.argv[1].lower() == "help":
        cli.help_fn([])
        exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--backend", type=str, default="sqlite", help="Supported backends are sqlite, duckdb, and ndp")

    args = parser.parse_args()
    if args.backend.lower() not in ["sqlite", "duckdb", "ndp"]:
        print("ERROR: Invalid backend input. Valid backends are: sqlite, duckdb, and ndp")
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
                print("Unknown command. Type \"help\" to see valid commands.\n")
                continue

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
