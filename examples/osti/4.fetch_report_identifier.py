from dsi.dsi import DSI
import pandas as pd

report_numbers = [
    "LA-UR-24-12345",
    "LA-UR-24-23456",
    "LA-UR-24-34567",
]

frames = []

for rn in report_numbers:
    dsi = DSI(
        backend_name="OSTI",identifier=rn,rows=10,silence_messages=True,
    )

    df = dsi.get_table("records", collection=True)

    if df is not None and not df.empty:
        df = df[df["report_number"] == rn]

        if not df.empty:
            df["queried_report_number"] = rn
            frames.append(df)

    dsi.close()

# Combine results
if frames:
    results = pd.concat(frames, ignore_index=True)

    print(results[[ "queried_report_number", "report_number", "osti_id", "title", "publication_date"]])
else:
    print("No matching reports found.")
