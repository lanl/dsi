import os
import shutil
from dsi.dsi import DSI

path = "./download_all/"

if os.path.exists(path):
    shutil.rmtree(path)

os.makedirs(path)

# Download every Oceans11 record and all associated T2 datasets.
dsi = DSI(
    backend_name="Oceans11",
    params={"download_all": True},
    workspace=path,
)

# Save the Tier 1 records table as a db.
# T2 db files should also be downloaded into workspace and linked by t2db_path.
dsi.process("sqlite", path + "oceans11_all.db")

dsi.summary()

dsi.close()