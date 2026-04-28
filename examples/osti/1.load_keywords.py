from dsi.dsi import DSI

# load OSTI backend and query for 5 entries with the keyword climate
# both formats work
#dsi = DSI(backend_name="OSTI", params={"q": "climate","rows": 5})
dsi = DSI(backend_name="OSTI", q="climate", rows=5)

# list the number of resulting entries
dsi.list()

# list the corresponding records
df = dsi.get_table("records", collection=True)
# print the metadata fields: osti_id, title and publication_date
print(df[["osti_id", "title", "publication_date"]].head())

dsi.close()
