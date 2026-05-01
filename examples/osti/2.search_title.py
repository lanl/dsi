# examples/osti/osti_user/2.search_title.py

from dsi.dsi import DSI

# Search OSTI records by title field
dsi = DSI(backend_name="OSTI", params={"title": "climate model","rows": 10})

df = dsi.get_table("records", collection=True)

print("\nRecords matching title search:")
print(df[["osti_id", "title", "publication_date"]])

dsi.summary()

dsi.close()


