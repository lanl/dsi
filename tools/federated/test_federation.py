import yaml
import sys
import uuid
from pathlib import Path

from git_utils import download_github_file, get_github_remote_file_size
from utils import create_directory, run, csv_to_list_of_dicts
from rsync_utils import rsync_download_interactive, rsync_remote_size_bytes_interactive


def main():
    # Make sure that we have a file
    if len(sys.argv) != 2:
        print(f"Usage: {Path(sys.argv[0]).name} <input.yaml>")
        sys.exit(1)

    # Read configuration from YAML file
    try:
        yaml_path = Path(sys.argv[1])
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Error: Could not find YAML file {yaml_path}")
        sys.exit(1)


    # initialize some variables
    repo_path = data["repo_path"]
    tmp_folder = f"_tmp_{uuid.uuid4().hex[:8]}"
    workspace_folder = f"_workspace_{uuid.uuid4().hex[:8]}"


    # create the workspace directory
    create_directory(workspace_folder)


    # clone the repo
    try:
        run(f"git clone {repo_path} {tmp_folder}")
    except RuntimeError as e:
        print(f"Error cloning repository: {e}")
        sys.exit(1)


    # Get the list of databases
    catalogue_index = f"{tmp_folder}/index_db.csv"
    catalogue_list = csv_to_list_of_dicts(catalogue_index)

    # Gather the databases and create the index database
    counter = 0
    for db in catalogue_list:
        location_type = db['location_type']
        location = db['location']
        path = db['path']

        print(f"\n - Processing database at {location_type}:{location}:{path}")

        if location_type == "github":
            filesize = 0
            # Check if the file exists and get its size
            try:
                filesize = get_github_remote_file_size(path)
            except Exception:
                print(f" -- Could not access the file at {path}. Skipping this database.")
                continue

            # Download the file
            try:
                download_github_file(url=path, out_path=workspace_folder)
                counter += 1
            except Exception as e:
                print(f" -- Error downloading file from GitHub: {e}")
                continue
            

        elif location_type == "HPC":
            filesize = 0
            # Check if the file exists and get its size
            try:
                filesize = rsync_remote_size_bytes_interactive(
                    remote=location,
                    remote_path=path
                )
            except FileNotFoundError as e:
                print(f" -- Could not access the file at {location}:{path}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Could not access the file at {location}:{path}. Skipping this database.")
                continue

            # Download the file
            try:
                rsync_download_interactive(
                    remote=location,
                    remote_path=path,
                    local_path=workspace_folder,
                )
                counter += 1
            except Exception as e:
                print(f" -- Error downloading file from HPC: {e}")
                continue

            
        else:
            print(f"Location type {location_type} for database {db} is unsupported. Skipping.")
            continue

    print(f"\nFinished gathering databases. Successfully downloaded {counter} files to {workspace_folder}.")

if __name__=="__main__":
    main()