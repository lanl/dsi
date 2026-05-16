import os
import shutil
import pandas as pd
from dsi.dsi import DSI

path = "./keyword/"

# If the directory exists, delete it and all its contents
if os.path.exists(path):
    shutil.rmtree(path)

# Create the directory (fresh)
os.makedirs(path)

# load OSTI backend and query for 5 entries with the keyword climate
dsi = DSI(backend_name="Oceans11", params={"q": "heat","rows": 5}, workspace=path)

# save the results as a db
dsi.process("sqlite", path + "heat.db")
# list the number of resulting entries
dsi.list()

# list the corresponding records
df = dsi.get_table("records", collection=True)

# print the metadata fields: osti_id, title and publication_date
print(df[["osti_id", "title", "publication_date"]].head())

dsi.close()
