from pyarrow import json
import yaml
import sys
import uuid
from pathlib import Path

from git_utils import download_github_file, get_github_remote_file_size
from utils import create_directory, run, csv_to_list_of_dicts, human_readable_size, get_last_part, create_folder_from_path, compute_md5, should_download
from rsync_utils import rsync_download_interactive, rsync_remote_size_bytes_interactive


def main():
    # Make sure that we have a file
    if len(sys.argv) not in (2, 3):
        print(f"Usage: {Path(sys.argv[0]).name} <input.yaml> [dsi_datasets_folder]")
        sys.exit(1)

    # Read configuration from YAML file
    try:
        yaml_path = Path(sys.argv[1])
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Error: Could not find YAML file {yaml_path}")
        sys.exit(1)


    if len(sys.argv) == 3:
        workspace_folder = sys.argv[2]
    else:
        _workspace_folder = data.get("workspace_folder", "")
        workspace_folder = _workspace_folder or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"

    print(f"Databases will be synchronized to: {workspace_folder}")


    # if it does not exist
    create_directory(workspace_folder)  


    # initialize some variables
    repo_path = data["repo_path"]
    tmp_folder = f"_tmp_{uuid.uuid4().hex[:8]}"


    # clone the repo pointing to by the yaml file
    try:
        run(f"git clone {repo_path} {tmp_folder}")
    except RuntimeError as e:
        print(f"Error cloning repository: {e}")
        sys.exit(1)


    # Get the list of databases
    catalogue_index = f"{tmp_folder}/index_db.csv"
    catalogue_list = csv_to_list_of_dicts(catalogue_index)


    # Create a list of hostnames and usernames
    try:
        with open(f"{_workspace_folder}/host_usernames.json", "r", encoding="utf-8") as f:
            host_username = yaml.safe_load(f)
    except Exception:
        #print(f"host_usernames.json does not exist. We will create a new one.")
        host_username = {}


    # Gather the databases and create the index database
    counter = 0
    for db in catalogue_list:
        location_type = db['location_type']
        location = db['location']
        path = db['path']
        filename = get_last_part(path)

        # Create folder for data
        abs_path_workspace_folder = str(Path(workspace_folder).resolve())         
        folder_name, abs_path_db_folder = create_folder_from_path(location, abs_path_workspace_folder)  

        # Get the absolute path to the file to be downloaded
        file_path = Path(abs_path_db_folder) / filename

        # Compute the MD5 hash of the existing file if it exists
        md5_file_hash = ""
        if file_path.exists():
            md5_file_hash = compute_md5(str(file_path))

            

        print(f"\n - Processing database at {location_type}:{location}:{path}")

        if location_type == "github":
            
            # Check if the file exists and get its size
            filesize = 0
            try:
                filesize = get_github_remote_file_size(path)
            except Exception:
                print(f" -- Could not access the file at {path}. Skipping this database.")
                continue



            # Confirm for sizes abouve a limit
            if filesize > data["download_limit"]:
                print(f"File size {human_readable_size(filesize)} bytes exceeds the download limit of {human_readable_size(data['download_limit'])} bytes.")
                print(f" -- Please confirm that you want to download this file (y/n): ", end="")
                choice = input().lower()
                if choice != 'y':
                    print(" -- Skipping this database.")
                    continue

            # Download the file
            try:
                download_github_file(url=path, out_path=abs_path_db_folder)
                counter += 1
            except Exception as e:
                print(f" -- Error downloading file from GitHub: {e}")
                continue
            

        elif location_type == "HPC":

            # Ask for username if we don't have it for this host yet
            if location not in host_username:                
                username = input(f" -- Enter the username for {location}: ")
                host_username[location] = username
            else:
                username = host_username[location]

            
            # Check if the file exists and get its size
            filesize = 0
            try:
                filesize = rsync_remote_size_bytes_interactive(
                    remote=f"{username}@{location}",
                    remote_path=path
                )
            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                continue
            except FileNotFoundError as e:
                print(f" -- Could not access the file at {location}:{path}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Could not access the file at {location}:{path}. Skipping this database.")
                continue



            # Check if the file already exists and has the same hash as the remote file
            if md5_file_hash != "":

                need_redownload = True
                try:
                    need_redownload = should_download(
                        remote=f"{username}@{location}",
                        remote_path=path,
                        stored_md5=md5_file_hash
                    )
                except KeyboardInterrupt:
                    print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                    continue
                except Exception as e:
                    print(f" -- Failed to get remote hash for {location}:{path}: {e}")
                    print(f" -- Will proceed to download the file to ensure we have the correct version.")

                if not need_redownload:
                    print(f" -- Local file is up to date with the remote file. Skipping download.")
                    continue



            # Confirm for sizes abouve a limit
            if filesize > data["download_limit"]:
                print(f"File size {human_readable_size(filesize)} bytes exceeds the download limit of {human_readable_size(data['download_limit'])} bytes.")
                print(f" -- Please confirm that you want to download this file (y/n): ", end="")
                choice = input().lower()
                if choice != 'y':
                    print(" -- Skipping this database.")
                    continue


            # Download the file
            try:
                rsync_download_interactive(
                    remote=f"{username}@{location}",
                    remote_path=path,
                    local_path=abs_path_db_folder
                )
                counter += 1
            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Error downloading file from HPC: {e}")
                continue
            
        else:
            print(f"Location type {location_type} for database {db} is unsupported. Skipping.")
            continue


    # Save host_usernames to a file for future runs
    with open(f"{abs_path_workspace_folder}/host_usernames.json", "w", encoding="utf-8") as f:
            yaml.safe_dump(host_username, f)


    print(f"\nFinished gathering databases. Successfully downloaded {counter} databases to {workspace_folder}.")


if __name__=="__main__":
    main()
