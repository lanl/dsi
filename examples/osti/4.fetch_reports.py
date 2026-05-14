from dsi.dsi import DSI

report_numbers = [
    "LA-UR-25-31939",
    "LA-UR-25-28633",
    "LA-UR-25-29620"
]

# Build batched params
params = [{"identifier": rn, "rows": 10} for rn in report_numbers]

dsi = DSI(backend_name="OSTI", params=params)

df = dsi.get_table("records", collection=True)

print(df[["report_number", "osti_id", "title", "publication_date"]])

dsi.close()