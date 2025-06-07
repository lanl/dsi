#!/usr/bin/env python3
"""
This script reads in the csv file created from parse_slurm_output.py.
Then it creates a DSI db from the csv file and performs a query.
"""
import os
from dsi.dsi import DSI

if __name__ == "__main__":
    test_name = "leblanc"
    table_name = "rundata"
    csvpath = f'pennant_{test_name}.csv'
    dbpath = f'pennant_{test_name}.db'
    datacard = "pennant_oceans11.yml"
    output_csv = "pennant_output.csv"

    if os.path.exists(dbpath):
        os.remove(dbpath)
    
    dsi = DSI(dbpath)

    dsi.read(csvpath, "Ensemble", table_name=table_name)
    dsi.read(datacard, "Oceans11Datacard")

    # saves query output as a Pandas DataFrame to then update that table in the backend
    query_output = dsi.query(f"SELECT * FROM {table_name} WHERE hydro_cycle_run_time > 0.006;", collection=True)
    dsi.update(query_output)

    dsi.write(output_csv, "Csv_Writer", table_name=table_name)