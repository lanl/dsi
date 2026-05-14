import os
import shutil
from dsi.dsi import DSI

path = "./title/"

# If the directory exists, delete it and all its contents
if os.path.exists(path):
    shutil.rmtree(path)

# Create the directory (fresh)
os.makedirs(path)

# Search OSTI records by title field
dsi = DSI(backend_name="OCEANS11", params={"title": "monopoly","rows": 10}, workspace=path)

# save the results as a db
dsi.process("sqlite", path + "monopoly.db")

dsi.summary()

dsi.close()


