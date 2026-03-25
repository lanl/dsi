# Federation for DSI

The allows users to pull database from many different locations and provide a centralized view of the databases. To run, from the DSI folder

```bash
python tools/federated/federate_dataset.py tools/federated/input.yaml
```

input.yaml contains the paths from which data can be pulled:
```yaml
remote_repo_path: "ssh://git@lisdi-git.lanl.gov:10022/personal/test_dsi_federated.git"
local_repo_path: "tools/federated/local_dsi_sources.csv"
workspace_folder: "dsi_databases_01"
download_limit: 10485760 # 10 MB
```

 - local_repo_path points to a CSV file where the user can specify local DSI repos
 - workspace_folder is where the remote federated datasets will be stored, those local to your computer will not be moved

 An example of local_repo_path is:
```csv
location_type,location,path,type,submitter_name,submitter_email,timsestamp
local,local,tools/federated/database/ocean_11_datasets.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:40:00s
local,local,/home/pascalgrosset/data/artimis/fracture/3d/aleks/fracture_aleks.sqlite,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:40:00s
HPC,ch-fe.lanl.gov,/lustre/scratch5/pascalgrosset/test_db/nif.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-3-10--16:38:00
url,url,https://www.timestored.com/data/sample/sakila.db,data,unknown,unknown,2026-3-10--16:38:00
```

The currently supported loication types are:
- local: refers to your local computer
- HPC: refers to a supercomupter
- url: a file on the web
- github: a file on a github repo