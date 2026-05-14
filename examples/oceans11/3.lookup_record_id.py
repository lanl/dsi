import os
import shutil
from dsi.dsi import DSI

path = "./recordID/"

# If the directory exists, delete it and all its contents
if os.path.exists(path):
    shutil.rmtree(path)

# Create the directory (fresh)
os.makedirs(path)

# Query OSTI records using osti_id through GET /records?osti_id=...
dsi = DSI(backend_name="Oceans11", params={"report_number": "LA-UR-21-30575","rows": 5}, workspace=path)

# save the results as a db
dsi.process("sqlite", path + "firetec.db")

dsi.summary()

dsi.close()

