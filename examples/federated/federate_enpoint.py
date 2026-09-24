import asyncio
import getpass
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime

from dsi.utils.data_acquisition import pull_data, discover_endpoints_async
from dsi.utils.acquisition.utils import (
    create_directory,
    combine_csv,
    create_hashed_folder_from_path,
    split_path,
    upsert_records,
)

# Configure logging - only to file, not console
log_filename = f"federate_endpoints_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename)
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"Starting federate_endpoints script. Logs will be saved to: {log_filename}")
print(f"Logs are being saved to: {log_filename}")

hpc_type = input("Enter the type of HPC (hpc, kerberos, ...): ")
hpc_name = input("Enter the name of the HPC: ")
username = input("Enter username: ")

# Optional: password for direct authentication
use_password = input("Use password authentication? (y/n, default: n): ").lower() == 'y'
password = getpass.getpass("Enter password: ") if use_password else None

script_path = input("Enter script path: ")

# Jump host support (for Kerberos with jump host)
jump_host = None
jump_username = None
jump_password = None
reticket_cmd = "reticket"

if hpc_type == 'kerberos':
    use_jump = input("Use jump host? (y/n, default: n): ").lower() == 'y'
    if use_jump:
        jump_host = input("  Jump host hostname: ")
        jump_username = input("  Jump host username: ")
        jump_password = getpass.getpass("  Jump host password: ")
        reticket_cmd = input("  Kerberos ticket-init command (default: reticket): ") or "reticket"

# currently a script setting environment variables but should be load module in the future
#script_path='/lustre/roscratch1/pascalgrosset/dsi_test/load_dsi_endpoints.sh'

# prefix of the endpoints; environment variables to search for
prefixes = ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']

endpoints_location = asyncio.run(discover_endpoints_async(
    hostname=hpc_name,
    username=username,
    script_path=script_path,
    prefixes=prefixes,
    password=password,
    hpc_type=hpc_type,
    jump_host=jump_host,
    jump_username=jump_username,
    jump_password=jump_password,
    reticket_cmd=reticket_cmd,
    logger=logger
))
logger.info(f"Retrieved endpoints: {endpoints_location}")

if endpoints_location == {}:
    print(f"No endpoints found at {script_path}!")
    sys.exit(0)
else:
    print(f"Endpoint locations: {endpoints_location}")

# Federate the data in specified folder
rel_wrks_folder = input("\nEnter the name of the folder (on your local computer) to federate to: ") #"test_federate_07"
workspace_folder = str(Path(rel_wrks_folder).resolve())
logger.info(f"Workspace folder: {workspace_folder}")

# Whether this HPC requires a jump host for downloads (mirrors discovery)
jump_host_required = hpc_type == 'kerberos' and bool(jump_host)

# Step 1: download the metadata CSV for each endpoint
temp_db_storage = ".test_00"
create_directory(dir_name=temp_db_storage, delete_if_exists=True, verbose=True)

print("\n\nPull Remote DBs ... ")
downloaded_files = []
for endpoint_name, endpoint_csv_path in endpoints_location.items():
    print(f"Retrieving data for {endpoint_name} at {endpoint_csv_path}")
    try:
        downloaded_file = pull_data(
            location_type="hpc",
            remote_location=hpc_name,
            remote_path=endpoint_csv_path,
            download_location=temp_db_storage,
            username=username,
            password=password,
            jump_host=jump_host,
            jump_username=jump_username,
            jump_password=jump_password,
            jump_host_required=jump_host_required,
            reticket_cmd=reticket_cmd
        )
        if downloaded_file:
            downloaded_files.append(downloaded_file)
    except Exception as e:
        print(f"Error accessing {endpoint_name}: {e}")
        print(f"   Skipping {endpoint_name} and continuing with remaining endpoints...\n")
        continue

if not downloaded_files:
    print("\nWarning: No endpoint metadata files were successfully downloaded.")
    print("   No databases will be federated.")
    sys.exit(1)

# Step 2: combine the downloaded CSVs into a single list of data sources
output_csv = str(Path(temp_db_storage) / "output_csv.csv")
try:
    csv_data_sources = combine_csv(temp_db_storage, output_csv)
except ValueError as e:
    print(f"\nWarning: {e}")
    print("   No databases will be federated.")
    sys.exit(1)

# Step 3: download each database, prompting for per-host credentials as needed
print("\n\nRead Data from endpoints ... ")
database_info = []
success_count = 0
host_credentials = {}

for row in csv_data_sources:
    location_type = row['location_type'].strip().lower()
    location = row['location']
    remote_path = row['path']

    db_username = ""
    db_password = ""
    if location_type == "hpc":
        if location not in host_credentials:
            print(f"\n{'='*60}")
            print(f"Enter credentials for data at {location} : {remote_path}")
            try:
                entered_username = input("Enter username: ")
                entered_password = getpass.getpass("Enter password: ")
            except Exception:
                print("... skipping and continue to the next ...")
                continue
            host_credentials[location] = (entered_username, entered_password)
        db_username, db_password = host_credentials[location]

    try:
        folder_hash = create_hashed_folder_from_path(remote_path, workspace_folder)[0]

        downloaded_file_path = pull_data(
            location_type=location_type,
            remote_location=location,
            remote_path=remote_path,
            download_location=(workspace_folder + '/' + folder_hash),
            username=db_username,
            password=db_password,
            jump_host=jump_host if location == hpc_name else None,
            jump_username=jump_username if location == hpc_name else None,
            jump_password=jump_password if location == hpc_name else None,
            jump_host_required=jump_host_required if location == hpc_name else False,
            reticket_cmd=reticket_cmd
        )

        if downloaded_file_path:
            _local_folder, _local_filename = split_path(downloaded_file_path)
            db_info = {
                "original_location_type": location_type,
                "original_path": remote_path,
                "folder_hash": folder_hash,
                "local_path": _local_folder,
                "name": _local_filename,
            }
            database_info.append(db_info)
            success_count += 1
    except Exception as e:
        print(f"Warning: Skipping database at {location}:{remote_path} due to error: {e}")
        print(f"   Continuing with remaining databases...\n")
        continue

upsert_records(f"{workspace_folder}/dsi_database_list.json", database_info, key="original_path")
shutil.rmtree(temp_db_storage, ignore_errors=True)

logger.info(f"Completed pulling data. Success count: {success_count}")

if success_count == 0:
    print("\nNo databases were successfully federated.")
    print("   Please check your credentials and network connection.")
    print(f"\nCheck the log file for details: {log_filename}")
    sys.exit(1)
else:
    print(f"\nSuccessfully gathered {success_count} database(s) to {workspace_folder}")
    print(f"   Database info: {database_info}")
    print(f"\nComplete log saved to: {log_filename}")

# /vast/home/pascalgrosset/dsi_sources_tests/load_dsi_endpoints.sh

# In load_dsi_endpoints.sh
# export DSI_ENDPOINT_CHICOMA_1="/users/pascalgrosset/dsi_test/dsi_hpc_sources.csv"
# export DSI_ENDPOINT_CHICOMA_2="/users/pascalgrosset/dsi_test/dsi_online_sources.csv"

# In dsi_test/dsi_hpc_sources.csv
# location_type,location,path,type,submitter_name,submitter_email,timestamp
# HPC,ch-fe.lanl.gov,/lustre/scratch5/pascalgrosset/test_db/nif.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-3-10--16:38:00
# HPC,darwin-fe.lanl.gov,/users/pulido/modelcard2.db,model,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:42:00
# HPC,darwin-fe.lanl.gov,/vast/projects/exasky/pascal/genesis_model_cards/modelcard.db,model,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:40:00s%

# In dsi_online_sources.csv:
# location_type,location,path,type,submitter_name,submitter_email,timestamp
# url,url,https://www.timestored.com/data/sample/sakila.db,data,unknown,unknown,2026-3-10--16:38:00
# url,url,https://oceans11.lanl.gov/dataCatalog/oceans11.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:30:00

## Endpoints
# /users/pascalgrosset/dsi_test/load_dsi_endpoints.sh  ch-fe.lanl.gov
# /vast/home/pascalgrosset/dsi_sources_tests/load_dsi_endpoints.sh darwin-fe.lanl.gov
# /g/g92/grosset2/dsi_test/load_dsi_endpoints.sh  tuolumne.llnl.gov
# /users/pascalgrosset/dsi_test/load_dsi_endpoints.sh ro-rfe.lanl.gov
