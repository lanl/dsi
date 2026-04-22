import sys
import uuid
import yaml
from pathlib import Path


from dsi.utils.federation_utils import (
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

from dsi.utils.git_utils import download_github_file, get_github_remote_file_size
from dsi.utils.rsync_utils import rsync_download_interactive, ssh_remote_size_bytes_interactive
from dsi.utils.web_utils import download_web_file, get_url_file_size
from dsi.utils.s3_utils import download_s3_file, get_s3_remote_file_size, resolve_s3_bucket_and_key, should_download_s3, get_s3_client


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



def pull_data(location_type: str, 
              location: str, 
              path: str, 
              abs_path_workspace_folder: str, 
              host_username: dict,
              download_limit: int) -> dict | None:
    """Pulls data from a specified location based on the location type (e.g., "github", "HPC", "URL", "local"). 
    The function checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly. 
    It also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        location_type (str): The type of the original location (e.g., "github", "HPC", "URL", "local").
        location (str): The location of the database (e.g., hostname for HPC, URL for web).
        path (str): The path to the database at the original location.
        abs_path_workspace_folder (str): The absolute path to the workspace folder where the database will be stored.
        host_username (dict): A dictionary mapping hostnames to usernames for HPC access.
        download_limit (int): The maximum size of a file that can be downloaded without confirmation.
    Returns:
        dict | None: A dictionary containing the database information if the data was successfully pulled, or None if there was an error or if the user chose to skip the download.
    """

    cleaned_location_type = location_type.strip().lower()
    filename = get_last_part(path)

    # Create folder for data        
    _, abs_path_db_folder = create_folder_from_path(location, abs_path_workspace_folder)  

    # Get the absolute path to the file to be downloaded
    file_path = Path(abs_path_db_folder) / filename
    
    # remove extra spaces
    path = path.strip()


    # Compute the MD5 hash of the existing file if it exists
    md5_file_hash = ""
    if file_path.exists():
        md5_file_hash = compute_md5(str(file_path))

    print(f"\n\n - Processing database at {location_type}:{location}:{path}")


    if cleaned_location_type == "github":
        
        # Check if the file exists and get its size
        filesize = 0
        try:
            filesize = get_github_remote_file_size(path)
        except Exception:
            print(f" -- Could not access the file at {path}. Skipping this database.")
            return None

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return None

        # Download the file
        try:
            download_github_file(url=path, out_path=abs_path_db_folder)

            db_info = make_db_info(location, path, file_path, filename)
            return db_info
            
        except Exception as e:
            print(f" -- Error downloading file from GitHub: {e}. Skipping this database.")
            return None
        

    elif cleaned_location_type == "hpc":

        # Ask for username if we don't have it for this host yet
        if location not in host_username:
            try:
                username = input(f" -- Enter the username for {location}: ")
                host_username[location] = username
            except KeyboardInterrupt:
                print(f"\n -- Interrupted while entering username for {location}. Skipping this database.")
                return None
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
            return None
        except FileNotFoundError as e:
            print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
            return None
        except Exception as e:
            print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
            return None


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
                return None
            except Exception as e:
                print(f" -- Failed to get remote hash for {location}:{path}: {e}")
                print(" -- Will proceed to download the file to ensure we have the correct version.")
            if not need_redownload:
                print(" -- Local file is up to date with the remote file. Skipping download.")
                return None

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return None

        # Download the file
        try:
            rsync_download_interactive(
                remote=f"{username}@{location}",
                remote_path=path,
                local_path=abs_path_db_folder
            )

            db_info = make_db_info(location, path, file_path, filename)
            return db_info

        except KeyboardInterrupt:
            print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
            return None
        except Exception as e:
            print(f" -- Error {e} downloading file from HPC. Skipping this database.")
            return None
        

    elif cleaned_location_type == "url":

        filesize = 0
        try:
            filesize = get_url_file_size(path)
        except Exception:
            print(f" -- Could not access the file at {path}. Skipping this database.")
            return None

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return None

        # Download the file
        try:
            download_web_file(url=path, output_dir=abs_path_db_folder)

            db_info = make_db_info(location, path, file_path, filename)
            return db_info

        except Exception as e:
            print(f" -- Error {e} downloading file at {path}. Skipping this database.")
            return None
    

    elif cleaned_location_type == "s3":
        try:
            bucket, key = resolve_s3_bucket_and_key(location=location, path=path)
        except ValueError as e:
            print(f" -- Invalid S3 location/path: {e}. Skipping this database.")
            return None

        aws_region = "us-gov-west-1"
        aws_profile = None

        try:
            s3_client = get_s3_client(
                region_name=aws_region,
                profile_name=aws_profile,
                allow_anonymous=False,
                interactive=True,
            )
        except Exception as e:
            print(f" -- Could not initialize S3 client: {e}. Skipping this database.")
            return None

        try:
            filesize = get_s3_remote_file_size(bucket=bucket, key=key, s3_client=s3_client)
        except PermissionError as e:
            print(f" -- Permission error: {e}. Skipping this database.")
            return None
        except FileNotFoundError as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            return None
        except Exception as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            return None

        if md5_file_hash != "":
            try:
                need_redownload = should_download_s3(
                    bucket=bucket,
                    key=key,
                    stored_md5=md5_file_hash,
                    s3_client=s3_client,
                )
                if not need_redownload:
                    print(" -- Local file is up to date with the S3 object. Skipping download.")
                    return None
            except Exception as e:
                print(f" -- Failed to compare local file with S3 object s3://{bucket}/{key}: {e}")
                print(" -- Will proceed to download the file to ensure we have the correct version.")

        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return None

        try:
            downloaded_path = download_s3_file(
                bucket=bucket,
                key=key,
                output_dir=abs_path_db_folder,
                s3_client=s3_client,
            )
            db_info = make_db_info(
                f"s3://{bucket}",
                f"s3://{bucket}/{key}",
                downloaded_path,
                Path(downloaded_path).name,
            )
            return db_info
        except Exception as e:
            print(f" -- Error downloading file from S3: {e}. Skipping this database.")
            return None


    elif cleaned_location_type == "local":
        # Check if the file exists
        if not Path(path).exists():
            print(f" -- Local file {path} does not exist. Skipping this database.")
            return None

        # Check if it's a file
        if not Path(path).is_file():
            print(f" -- Local path {path} is not a file. Skipping this database.")
            return None


        _abs_path = str(Path(path).resolve())
        db_info = make_db_info(location, _abs_path, _abs_path, filename)
        return db_info


    else:
        print(f"Location type {location_type} for database {path} is unsupported. Skipping.")

    return None

    

def federate_datasets(workspace_folder: str, config_data: dict, yaml_path: str) -> None:
    """Federates datasets from various sources (local, GitHub, HPC, URL) based on the provided configuration.
      It checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly.
      The function also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        workspace_folder (str): The local folder where the datasets will be stored.
        config_data (dict): A dictionary containing configuration data, including repository paths and download limits.
        yaml_path (str): The path to the YAML configuration file, used for resolving relative paths in the configuration.
    """

    # Create the workspace folder if it doesn't exist
    abs_path_workspace_folder = str(Path(workspace_folder).resolve()) 
    create_directory(abs_path_workspace_folder)  
    print(f"Databases will be synchronized to: {abs_path_workspace_folder}")


    # Get the list of repos
    db_catalogue_list = []

    for repo in config_data.get("repo_paths", []):
        if Path(repo).is_absolute():
            repo_path = Path(repo)
        else:
            repo_path = Path(yaml_path) / repo

        clean_repo_path = str(repo_path.resolve())

        if clean_repo_path.endswith(".csv"):
            try:
                _temp_catalogues = csv_to_list_of_dicts(clean_repo_path)
                db_catalogue_list.extend(_temp_catalogues)
            except Exception as e:
                print(f"Error reading local repository {clean_repo_path}: {e}")
        else:
            print(f"Unsupported repository type for {clean_repo_path}. Only CSV files are supported for local repositories. Skipping this repo.")

    
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

        db_info = pull_data(
            location_type=db['location_type'],
            location=db['location'],
            path=db['path'],
            abs_path_workspace_folder=abs_path_workspace_folder,
            host_username=host_username,
            download_limit=config_data["download_limit"]
        )

        if db_info is not None:
            database_info.append(db_info)
            success_counter += 1


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

    yaml_folder = Path(yaml_path).parent
    federate_datasets(workspace_folder, config_data, str(yaml_folder))

  

if __name__=="__main__":
    main()


# Run as: python dsi/tools/federated/federate_dataset.py examples/federated/input.yaml