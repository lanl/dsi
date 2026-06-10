import os
import glob
import shutil

from dsi.dsi import DSI
from urllib.parse import urlparse
# from dsi.utils.web_utils import download_web_file
from dsi.utils.federated.federate_datasets import pull_data
from dsi.utils.federation_utils import create_directory, upsert_records

# import ssl
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

path = "./recordID/"

### Download the firetec T1 details and T2 db from Oceans11 ###

# If the directory exists, delete it and all its contents
if os.path.exists(path):
    shutil.rmtree(path)

# Create the directory (fresh)
os.makedirs(path)

# Query OSTI records using osti_id through GET /records?osti_id=...
dsi_db = DSI(backend_name="Oceans11", params={"report_number": "LA-UR-21-30892","rows": 5}, workspace=path)

# save the results as a db
dsi_db.process("sqlite", path + "firetec_search.db")

dsi_db.close()

### Now navigate through the data to identify the T2 database. ###

dsi_db = DSI(path+"firetec_search.db")
df = dsi_db.query("SELECT t2db_path FROM records", collection=True)
t2_db_path = df.iat[0, 0]
dsi_db.close()

### Now find the files to download. Here we show an SQL query that downloads all files at timestep 2000 ###
dsi_db = DSI(t2_db_path)
df = dsi_db.query("SELECT url FROM files WHERE timestep = 2000", collection=True) 
dsi_db.close()

### Download the files ###
urls = df['url'].tolist()
files = "./downloaded_files/"
host_username = {}
download_limit = 10**12  # 1 TB

# If the directory exists, delete it and all its contents
if os.path.exists(files):
    shutil.rmtree(files)
# Create the directory (fresh)
os.makedirs(files)

for url in urls:
    parsed = urlparse(url)

    url_parts = parsed.path.strip("/").split("/")
    filename = url_parts[-1]

    url_parent = "/".join(url_parts[:-1])

    relative_folder = os.path.join(*url_parts[:-1])
    final_folder = os.path.join(files, relative_folder)

    os.makedirs(final_folder, exist_ok=True)

    print(f"Downloading: {url}", flush=True)

    before = set(glob.glob(os.path.join(files, "*")))

    pull_data(
        location_type="url",
        location=url_parent,
        path=url,
        abs_path_workspace_folder=files,
        username=host_username,
        download_limit=download_limit,
    )

    after = set(glob.glob(os.path.join(files, "*")))
    new_dirs = list(after - before)

    if len(new_dirs) != 1:
        raise RuntimeError("Could not identify hashed download folder")

    hashed_dir = new_dirs[0]
    downloaded_file = os.path.join(hashed_dir, filename)
    final_path = os.path.join(final_folder, filename)

    shutil.move(downloaded_file, final_path)
    shutil.rmtree(hashed_dir)

    print(f"Saved to: {final_path}", flush=True)

print("DONE!")
