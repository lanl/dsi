# Federation for DSI

The allows users to pull database from many different locations and provide a centralized view of the databases. To run, from the DSI folder

To run:
```bash
python dsi/tools/federated/federate_datasets.py examples/federated/input.yaml
```
where input.yaml is a config file


## Config file
input.yaml is the config file that contains the paths from which data can be pulled:
```yaml
repo_paths: 
    - "~/remote_sources.csv"
    - "local_dsi_sources.csv"
workspace_folder: "dsi_databases_01"
download_limit: 10485760 # 10 MB
```
 - repo_paths: points to CSV files where the user can specify DSI repos. The paths should be relative to the config file or absolute paths
 - workspace_folder: is where the remote federated datasets will be stored, those local to your computer will not be moved
 - download_limit: after this file limit, the user will be asked to confirm


## Data Sources

 An example of database sources is as follows:
```csv
location_type,location,path,type,submitter_name,submitter_email,timsestamp
local,local,tools/federated/database/ocean_11_datasets.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:40:00s
local,local,/home/pascalgrosset/data/artimis/fracture/3d/aleks/fracture_aleks.sqlite,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:40:00s
HPC,ch-fe.lanl.gov,/lustre/scratch5/pascalgrosset/test_db/nif.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-3-10--16:38:00
url,url,https://www.timestored.com/data/sample/sakila.db,data,unknown,unknown,2026-3-10--16:38:00
```

**Note:** only location_type, location, and path are required

- location_type: the currently supported location_type are:
  - local: refers to your local computer
  - HPC: refers to a supercomupter
  - url: a file on the web
  - github: a file on a github repo

- location: on HPC systems, indicates the name of the cluster the data is on. For the others, it is the same as location_type
- path: the path of the dataset


## Other Notes:
- local repos will not be downloaded, there will just be a reference to them
- a file called dsi_database_list.json will be created that will show all the paths of the files downloaded.