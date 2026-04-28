from dsi.dsi import DSI

report_numbers = [
    "LA-UR-25-31939",
    "LA-UR-25-28633",
    "LA-UR-25-29620"
]

query = " OR ".join(f'"{rn}"' for rn in report_numbers)

dsi = DSI( backend_name="OSTI", q=query, rows=100)

df = dsi.get_table("records", collection=True)

# enforce exact report-number matches locally
results = df[df["report_number"].isin(report_numbers)]

print(results[["report_number", "osti_id", "title", "publication_date"]])

dsi.close()
