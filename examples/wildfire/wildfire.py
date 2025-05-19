import os
from dsi.dsi import DSI

isVerbose = True

if __name__ == "__main__":
    input_csv = "wildfiredataSmall.csv"
    db_name = 'wildfire.db'
    output_csv = "wildfire_output.csv"
    datacard = "wildfire_oceans11.yml"
    table_name = "wfdata"
    columns_to_keep = ["wind_speed", "wdir", "smois", "burned_area", "FILE"]

    dsi = DSI()

    dsi.read(input_csv, "Ensemble", table_name=table_name)
    dsi.read(datacard, "Oceans11Datacard")
    dsi.write(output_csv, "Csv_Writer", table_name=table_name)

    if os.path.exists(db_name):
        os.remove(db_name)
    
    dsi.backend(db_name, backend_name="Sqlite")
    dsi.ingest()
    dsi.display(table_name, display_cols=columns_to_keep)

