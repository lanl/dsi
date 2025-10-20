#!/usr/bin/env python3
"""
This script reads in the csv file created from parse_slurm_output.py.
Then it creates a DSI db from the csv file and performs a query.
"""
import os
from dsi.core import Terminal

if __name__ == "__main__":
    test_name = "leblanc"
    table_name = "rundata"
    csvpath = f'pennant_{test_name}.csv'
    dbpath = f'pennant_{test_name}.db'
    datacard = "pennant_oceans11.yml"
    output_csv = "pennant_output.csv"

    core = Terminal()

    # This reader creates a manual simulation table where each row of Pennant is its own simulation
    core.load_module('plugin', "Ensemble", "reader", filenames = csvpath, table_name = table_name, sim_table = True)
    core.load_module('plugin', "Oceans11Datacard", "reader", filenames = datacard)

    if os.path.exists(dbpath):
        os.remove(dbpath)

    #load data into sqlite db
    core.load_module('backend','Sqlite','back-write', filename=dbpath)
    core.artifact_handler(interaction_type='ingest')

    # update dsi abstraction using a query to the sqlite db
    query_data = core.artifact_handler(interaction_type='query', query = f"SELECT * FROM {table_name} WHERE hydro_cycle_run_time > 0.006;", dict_return = True)
    core.update_abstraction(table_name, query_data)

    #export to csv
    core.load_module('plugin', "Csv_Writer", "writer", filename = output_csv, table_name = table_name)
    core.transload()