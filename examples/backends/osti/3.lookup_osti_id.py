from dsi.dsi import DSI

# Query OSTI records using osti_id through GET /records?osti_id=...
dsi = DSI(backend_name="OSTI", params={"osti_id": "1234567","rows": 5})

df = dsi.get_table("records", collection=True)

print("\nOSTI ID query results:")
print(df[["osti_id", "title", "doi", "publication_date"]])

dsi.summary()

dsi.close()

