import getpass
import sys
import logging
from pathlib import Path
from datetime import datetime

from dsi.utils.data_acquisition import (
    get_remote_endpoints_ssh,
    pull_data_endpoints,
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

#password = getpass.getpass("Enter password: ")
script_path = input("Enter script path: ")


# currently a script setting environment variables but should be load module in the future
#script_path='/lustre/roscratch1/pascalgrosset/dsi_test/load_dsi_endpoints.sh' 

# prefix of the endpoints; environment variables to search for
prefixes=['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'] 

endpoints_location = get_remote_endpoints_ssh(hostname=hpc_name, 
                                              username=username, 
                                              script_path=script_path, 
                                              prefixes=prefixes,
                                              verbose=True)
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


database_info, success_count = pull_data_endpoints(endpoints_location, hpc_name, workspace_folder)
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

# /users/pascalgrosset/dsi_test/load_dsi_endpoints.sh
# /vast/home/pascalgrosset/dsi_sources_tests/load_dsi_endpoints.sh