import pandas as pd
from dsi.dsi import DSI

# load OSTI backend and query for 5 entries with the keyword climate
dsi = DSI(backend_name="OSTI", params={"q": "climate","rows": 5})

# save the results as a db
dsi.process("sqlite", "climate.db")
# list the number of resulting entries
dsi.list()

# list the corresponding records
df = dsi.get_table("records", collection=True)

# print the metadata fields: osti_id, title and publication_date
print(df[["osti_id", "title", "publication_date"]].head())



dsi.close()
