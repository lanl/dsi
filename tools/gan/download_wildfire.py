from dsi.sync import Sync
from dsi.dsi import DSI
import json
import yaml
import subprocess
import os

bash_script = "download_csv"
federated_folder = ".gan_db/"
remote_file_list = ".files_to_download.txt"
gan_yaml = ".gan_sources.yaml"
gan_csv = "gan_data_path.csv"

# Download csv
if os.name != 'nt': # skip if on Windows
    subprocess.run(["bash", bash_script+".sh"], check=True)
else:
    subprocess.run(["cmd", "/c", bash_script+".bat"], check=True)


# Create yaml file
data = {
    "repo_paths": [gan_csv],
    "download_limit": 10485760,
    "conflict_resolution": "keep_latest"
}
with open(gan_yaml, "w") as f:
    yaml.dump(data, f)


# Download federated db
s = Sync()
try:
    s.get(gan_yaml, federated_folder)
except Exception as e:
    raise RuntimeError("Error accessing remote server. Try again later.")


# Get full hostname
with open(f"{federated_folder}/host_usernames.json") as f:
    data = yaml.safe_load(f)
host, username = next(iter(data.items()))
hostname = f"{username}@{host}:"


# Open db, create list of files to download, and download those files
data = json.load(open(f"{federated_folder}/dsi_database_list.json"))
gan_db = data[0]["local_path"]

store = DSI(gan_db)
filesystem_df = store.get_table("filesystem", True)

remote_prefix = "/".join(filesystem_df["file_remote"].iloc[0].split("/")[:5]) + "/"

filesystem_df["file_remote"] = filesystem_df["file_remote"].str.replace(f"^{remote_prefix}", "", regex=True)
filesystem_df["file_remote"].to_csv(remote_file_list, index=False, header=False)

subprocess.run(["rsync", "-av", f"--files-from={remote_file_list}", hostname+remote_prefix, "."])


# optionally delete temporarily created files and folders
subprocess.run(["rm", remote_file_list], check=True)
subprocess.run(["rm", gan_yaml], check=True)
subprocess.run(["rm", gan_csv], check=True)
subprocess.run(["rm", "-rf", federated_folder], check=True)