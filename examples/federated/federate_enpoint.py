import getpass
import sys
from pathlib import Path

from dsi.utils.federated.federate_datasets import (
    get_remote_endpoints_ssh,
    pull_data_endpoints,
)

hpc_name = input("Enter the name of the HPC: ")
username = input("Enter username: ")
#password = getpass.getpass("Enter password: ")
script_path = input("Enter script path: ")


# currently a script setting environment variables but should be load module in the future
#script_path='/lustre/roscratch1/pascalgrosset/dsi_test/load_dsi_endpoints.sh' 

# prefix of the endpoints; environment variables to search for
prefixes=['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'] 

endpoints_location = get_remote_endpoints_ssh(hpc_name, username, script_path, prefixes)

if endpoints_location == {}:
    print(f"No endpoints found at {script_path}!")
    sys.exit(0)


# Federate the data in specified folder
rel_wrks_folder = input("Enter the name of the folder (on your local computer) to federate to: ") #"test_federate_07"
workspace_folder = str(Path(rel_wrks_folder).resolve())

database_info = pull_data_endpoints(endpoints_location, hpc_name, workspace_folder)

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