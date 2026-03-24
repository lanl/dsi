from pyarrow import csv, json
import yaml
import sys
import uuid
from pathlib import Path

from git_utils import download_github_file, get_github_remote_file_size
from rsync_utils import rsync_download_interactive, rsync_remote_size_bytes_interactive
from web_utils import download_web_file, get_url_file_size
from federation_utils import compute_md5, create_directory, create_folder_from_path, csv_to_list_of_dicts, get_last_part, human_readable_size, run_shell_cmd, should_download, upsert_records


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



    # Create a folder for the databases if it doesn't exist, or use the provided one
    if len(sys.argv) == 3:
        workspace_folder = sys.argv[2]
    else:
        _workspace_folder = data.get("workspace_folder", "")
        workspace_folder = _workspace_folder or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"

    abs_path_workspace_folder = str(Path(workspace_folder).resolve()) 
    create_directory(abs_path_workspace_folder)  
    print(f"Databases will be synchronized to: {abs_path_workspace_folder}")



    # Get the list of repos
    catalogue_list = []

    # local repo by yaml
    if "local_repo_path" in data:
        local_repo_path = data["local_repo_path"]

        _temp_local_catalogues = csv_to_list_of_dicts(local_repo_path)
        catalogue_list.extend(_temp_local_catalogues)

    # clone the repo pointed to by the yaml file
    if "remote_repo_path" in data:
        repo_path = data["remote_repo_path"]

        tmp_folder = f"_tmp_{uuid.uuid4().hex[:8]}"
        try:
            run_shell_cmd(f"git clone {repo_path} {tmp_folder}")

            # Get the list of databases
            catalogue_index = f"{tmp_folder}/index_db.csv"

            _temp_git_catalogues = csv_to_list_of_dicts(catalogue_index)
            catalogue_list.extend(_temp_git_catalogues)
        except RuntimeError as e:
            print(f"Error cloning repository: {e}")

    print("Number of repos found: ", len(catalogue_list))
    


    # Create/open the list of hostnames and usernames
    try:
        with open(f"{abs_path_workspace_folder}/host_usernames.json", "r", encoding="utf-8") as f:
            host_username = yaml.safe_load(f)
    except Exception:
        host_username = {}


    # information about each database
    database_info = []

    # Gather the databases and create the index database
    for counter, db in enumerate(catalogue_list):
        location_type = db['location_type']
        location = db['location']
        path = db['path']
        filename = get_last_part(path)

        # Create folder for data        
        db_name, abs_path_db_folder = create_folder_from_path(location, abs_path_workspace_folder)  

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


            # Confirm for sizes above a limit
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

                db_info = {
                    "original_location_type": location_type,
                    "original_path": path,
                    "local_path": abs_path_db_folder + "/" + db_name,
                    "name": db_name,
                }
                database_info.append(db_info)
                
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


            # Confirm for sizes above a limit
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

                db_info = {
                    "original_location_type": location_type,
                    "original_path": path,
                    "local_path": abs_path_db_folder + "/" + db_name,
                    "name": db_name,
                }
                database_info.append(db_info)

            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Error downloading file from HPC: {e}")
                continue
            


        elif location_type == "URL":

            filesize = 0
            try:
                filesize = get_url_file_size(path)
            except Exception:
                print(f" -- Could not access the file at {path}. Skipping this database.")
                continue


            # Confirm for sizes above a limit
            if filesize > data["download_limit"]:
                print(f"File size {human_readable_size(filesize)} bytes exceeds the download limit of {human_readable_size(data['download_limit'])} bytes.")
                print(f" -- Please confirm that you want to download this file (y/n): ", end="")
                choice = input().lower()
                if choice != 'y':
                    print(" -- Skipping this database.")
                    continue


            # Download the file
            try:
                download_web_file(url=path, output_path=abs_path_db_folder)
                db_info = {
                    "original_location_type": location_type,
                    "original_path": path,
                    "local_path": abs_path_db_folder + "/" + db_name,
                    "name": db_name,
                }
                database_info.append(db_info)

            except Exception as e:
                print(f" -- Error downloading file from GitHub: {e}")
                continue
        


        elif location_type == "local":
            # Check if the file exists
            if not Path(path).exists():
                print(f" -- Local file {path} does not exist. Skipping this database.")
                continue

            # Check if it's a file
            if not Path(path).is_file():
                print(f" -- Local path {path} is not a file. Skipping this database.")
                continue


            db_info = {
                "original_location_type": location_type,
                "original_path": path,
                "local_path": path,
                "name": db_name,
            }
            database_info.append(db_info)


        else:
            print(f"Location type {location_type} for database {db} is unsupported. Skipping.")
            continue



    # Save host_usernames to a file for future runs
    with open(f"{abs_path_workspace_folder}/host_usernames.json", "w", encoding="utf-8") as f:
            yaml.safe_dump(host_username, f)

    # Save databases information to a JSON file
    upsert_records(f"{abs_path_workspace_folder}/host_usernames.json", database_info, key="original_path")


    print(f"\nFinished gathering databases. Successfully downloaded {counter} databases to {abs_path_workspace_folder}.")


if __name__=="__main__":
    main()
