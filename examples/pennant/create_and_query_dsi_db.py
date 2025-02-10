#!/usr/bin/env python3

"""
This script reads in the csv file created from parse_slurm_output.py.
Then it creates a DSI db from the csv file and performs a query.
"""

import argparse
import sys
from dsi.backends.sqlite import Sqlite, DataType
import os
from dsi.core import Terminal

if __name__ == "__main__":
    """ The testname argument is required """
    parser = argparse.ArgumentParser()
    parser.add_argument('--testname', help='the test name')
    args = parser.parse_args()
    test_name = args.testname
    if test_name is None:
        parser.print_help()
        sys.exit(0)
    
    table_name = "rundata"
    csvpath = 'pennant_' + test_name + '.csv'
    dbpath = 'pennant_' + test_name + '.db'
    output_csv = "pennant_read_query.csv"

    #read in csv
    core = Terminal()

    # using Wildfire reader instead of Csv, because it can be used for any post-process data that is not meant for in-situ analysis
    # This reader creates a manual simulation table where each row of pennant is its own simulation
    core.load_module('plugin', "Wildfire", "reader", filenames = csvpath, table_name = table_name, sim_table = True)

    if os.path.exists(dbpath):
        os.remove(dbpath)

    #load data into sqlite db
    core.load_module('backend','Sqlite','back-write', filename=dbpath)
    # using 'ingest' instead of 'put' but both do the same thing -- 'ingest' is new name that will replace 'put'
    core.artifact_handler(interaction_type='ingest')

    # update dsi abstraction using a query to the sqlite db
    # using 'query' instead of 'get' but both do the same thing -- 'query' is new name that will replace 'get'
    query_data = core.artifact_handler(interaction_type='query', query = f"SELECT * FROM {table_name} WHERE hydro_cycle_run_time > 0.006;", dict_return = True)
    core.update_abstraction(table_name, query_data)

    #export to csv
    core.load_module('plugin', "Csv_Writer", "writer", filename = output_csv, table_name = table_name)
    core.transload()