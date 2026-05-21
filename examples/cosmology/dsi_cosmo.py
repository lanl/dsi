#!/usr/bin/env python3
"""
"""
import os
from dsi.dsi import DSI

if __name__ == "__main__":
    test_name = "scidac"
    table_name = "sim-ensemble"
    csvpath = f'cosmo_{test_name}.csv'
    dbpath = f'cosmo_{test_name}.db'
    output_csv = "cosmo_output.csv"

    if os.path.exists(dbpath):
        os.remove(dbpath)
    
    dsi = DSI(dbpath)

    dsi.read(csvpath, "Ensemble", table_name=table_name)

    # saves query output as a Pandas DataFrame to then update that table in the backend
    query_output = dsi.query(f"SELECT * FROM {table_name};", collection=True)
    dsi.update(query_output)

    dsi.write(output_csv, "Csv_Writer", table_name=table_name)