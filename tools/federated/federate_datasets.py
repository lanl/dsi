import sys
import uuid
import yaml
from pathlib import Path

from federation_utils import (
    compute_md5, 
    create_directory, 
    create_folder_from_path, 
    csv_to_list_of_dicts, 
    deduplicate_keep_latest, 
    get_last_part, 
    human_readable_size, 
    should_download, 
    upsert_records
)

from git_utils import download_github_file, get_github_remote_file_size
from rsync_utils import rsync_download_interactive, ssh_remote_size_bytes_interactive
from web_utils import download_web_file, get_url_file_size


def confirm_large_download(filesize: int, download_limit: int) -> bool:
    """Prompts the user to confirm the download of a file if its size exceeds a specified limit. The function displays the file size in a human-readable format and asks the user for confirmation before proceeding with the download.
    
    Args:        
        filesize (int): The size of the file in bytes.
        download_limit (int): The download limit in bytes. If the file size exceeds this limit, the user will be prompted for confirmation.

    Returns:
        bool: True if the user confirms the download, False otherwise.
    """

    if filesize <= download_limit:
        return True

    print(
        f"File size {human_readable_size(filesize)} exceeds the "
        f"download limit of {human_readable_size(download_limit)}."
    )
    choice = input(" -- Please confirm that you want to download this file (y/n): ").strip().lower()
    return choice == "y"



def make_db_info(location_type:str, path:str, local_path:str, db_name:str) -> dict:
    """Creates a dictionary containing information about a database, including its original location type, original path, local path, and name.
    
    Args:
     location_type (str): The type of the original location (e.g., "github", "HPC", "URL", "local").
        path (str): The original path to the database.
        local_path (str): The local path where the database is stored after downloading or copying.
        db_name (str): The name of the database.

    Returns:
        dict: A dictionary containing the database information with keys "original_location_type", "original_path", "local_path", and "name".
    
    """
    return {
        "original_location_type": location_type,
        "original_path": path,
        "local_path": str(local_path),
        "name": db_name,
    }



def federate_datasets(workspace_folder: str, config_data: dict) -> None:
    """Federates datasets from various sources (local, GitHub, HPC, URL) based on the provided configuration.
      It checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly.
      The function also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        workspace_folder (str): The local folder where the datasets will be stored.
        config_data (dict): A dictionary containing configuration data, including repository paths and download limits.
    """

    # Create the workspace folder if it doesn't exist
    abs_path_workspace_folder = str(Path(workspace_folder).resolve()) 
    create_directory(abs_path_workspace_folder)  
    print(f"Databases will be synchronized to: {abs_path_workspace_folder}")


    # Get the list of repos
    db_catalogue_list = []

    for repo in config_data.get("repo_paths", []):
        if repo.endswith(".csv"):
            try:
                _temp_catalogues = csv_to_list_of_dicts(repo)
                db_catalogue_list.extend(_temp_catalogues)
            except Exception as e:
                print(f"Error reading local repository {repo}: {e}")
        else:
            print(f"Unsupported repository type for {repo}. Only CSV files are supported for local repositories. Skipping this repo.")



    
    # Remove duplicates while keeping the latest entry for each unique path
        # TODO: Allow the user to choose which one to keep instead of just keeping the 
        # latest one or specify a resolution mode in the yaml file or allow user to keep both and rename them or ...
    cleaned_db_catalogue_list = deduplicate_keep_latest(db_catalogue_list)
    print("Number of repos found: ", len(cleaned_db_catalogue_list))


    # Create/open the list of hostnames and usernames
    try:
        with open(f"{abs_path_workspace_folder}/host_usernames.json", "r", encoding="utf-8") as f:
            host_username = yaml.safe_load(f)
    except Exception:
        host_username = {}


    # information about each database
    database_info = []

    
    # Gather the databases and create the index database
    success_counter = 0
    for db in cleaned_db_catalogue_list:
        #TODO: add retries and timeouts for all downloads

        location_type = db['location_type']
        cleaned_location_type = location_type.strip().lower()
        location = db['location']
        path = db['path']
        filename = get_last_part(path)

        # Create folder for data        
        _, abs_path_db_folder = create_folder_from_path(location, abs_path_workspace_folder)  

        # Get the absolute path to the file to be downloaded
        file_path = Path(abs_path_db_folder) / filename


        # Compute the MD5 hash of the existing file if it exists
        md5_file_hash = ""
        if file_path.exists():
            md5_file_hash = compute_md5(str(file_path))

        print(f"\n\n - Processing database at {location_type}:{location}:{path}:{cleaned_location_type}")


        if cleaned_location_type == "github":
            
            # Check if the file exists and get its size
            filesize = 0
            try:
                filesize = get_github_remote_file_size(path)
            except Exception:
                print(f" -- Could not access the file at {path}. Skipping this database.")
                continue


            # Confirm for sizes above a limit
            if not confirm_large_download(filesize, config_data["download_limit"]):
                print(" -- Skipping this database.")
                continue


            # Download the file
            try:
                download_github_file(url=path, out_path=abs_path_db_folder)

                db_info = make_db_info(location, path, file_path, filename)
                database_info.append(db_info)
                success_counter += 1
                
            except Exception as e:
                print(f" -- Error downloading file from GitHub: {e}")
                continue
            


        elif cleaned_location_type == "hpc":

            # Ask for username if we don't have it for this host yet
            if location not in host_username:
                try:
                    username = input(f" -- Enter the username for {location}: ")
                except KeyboardInterrupt:
                    print(f"\n -- Interrupted while entering username for {location}. Skipping this database.")
                    continue
                host_username[location] = username
            else:
                username = host_username[location]

            
            # Check if the file exists and get its size
            filesize = 0
            try:
                filesize = ssh_remote_size_bytes_interactive(
                    remote=f"{username}@{location}",
                    remote_path=path
                )
            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                continue
            except FileNotFoundError as e:
                print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
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
                    print(" -- Will proceed to download the file to ensure we have the correct version.")

                if not need_redownload:
                    print(" -- Local file is up to date with the remote file. Skipping download.")
                    continue


            # Confirm for sizes above a limit
            if not confirm_large_download(filesize, config_data["download_limit"]):
                print(" -- Skipping this database.")
                continue


            # Download the file
            try:
                rsync_download_interactive(
                    remote=f"{username}@{location}",
                    remote_path=path,
                    local_path=abs_path_db_folder
                )

                db_info = make_db_info(location, path, file_path, filename)
                database_info.append(db_info)
                success_counter += 1

            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
                continue
            except Exception as e:
                print(f" -- Error {e} downloading file from HPC. Skipping this database.")
                continue
            


        elif cleaned_location_type == "url":

            filesize = 0
            try:
                filesize = get_url_file_size(path)
            except Exception:
                print(f" -- Could not access the file at {path}. Skipping this database.")
                continue


            # Confirm for sizes above a limit
            if not confirm_large_download(filesize, config_data["download_limit"]):
                print(" -- Skipping this database.")
                continue


            # Download the file
            try:
                download_web_file(url=path, output_path=abs_path_db_folder)

                db_info = make_db_info(location, path, file_path, filename)
                database_info.append(db_info)
                success_counter += 1

            except Exception as e:
                print(f" -- Error {e} downloading file at {path}. Skipping this database.")
                continue
        


        elif cleaned_location_type == "local":
            # Check if the file exists
            if not Path(path).exists():
                print(f" -- Local file {path} does not exist. Skipping this database.")
                continue

            # Check if it's a file
            if not Path(path).is_file():
                print(f" -- Local path {path} is not a file. Skipping this database.")
                continue

            _abs_path = str(Path(path).resolve())
            db_info = make_db_info(location, _abs_path, _abs_path, filename)
            database_info.append(db_info)
            success_counter += 1


        else:
            print(f"Location type {location_type} for database {db} is unsupported. Skipping.")
            continue



    # Save host_usernames to a file for future runs
    with open(f"{abs_path_workspace_folder}/host_usernames.json", "w", encoding="utf-8") as f:
            yaml.safe_dump(host_username, f)

    # Save databases information to a JSON file
    upsert_records(f"{abs_path_workspace_folder}/dsi_database_list.json", database_info, key="original_path")


    print(f"\nFinished gathering databases. Successfully downloaded {success_counter} databases to {abs_path_workspace_folder}.")



def main():
    # Make sure that we have a file
    if len(sys.argv) not in (2, 3):
        print(f"Usage: {Path(sys.argv[0]).name} <input.yaml> [dsi_datasets_folder]")
        sys.exit(1)

    # Read configuration from YAML file
    try:
        yaml_path = Path(sys.argv[1])
        config_data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Error: Could not find YAML file {yaml_path}")
        sys.exit(1)

    
    # Create a folder for the databases if it doesn't exist, or use the provided one
    if len(sys.argv) == 3:
        workspace_folder = sys.argv[2]
    else:
        _workspace_folder = config_data.get("workspace_folder", "")
        workspace_folder = _workspace_folder or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"


    federate_datasets(workspace_folder, config_data)

    


if __name__=="__main__":
    main()


# Run as: python tools/federated/federate_dataset.py tools/federated/input.yaml