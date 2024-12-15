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

isVerbose = True

"""
Creates the DSI db from the csv file
"""

# def import_pennant_data(test_name):
#     csvpath = 'pennant_' + test_name + '.csv'
#     dbpath = 'pennant_' + test_name + '.db'
#     store = Sqlite(dbpath)
#     store.put_artifacts_csv(csvpath, "rundata", isVerbose=isVerbose)
#     store.close()
#     # No error implies success

"""
Performs a sample query on the DSI db
"""
# def test_artifact_query(test_name):
#     dbpath = "pennant_" + test_name + ".db"
#     store = Sqlite(dbpath)
#     _ = store.get_artifacts(isVerbose=isVerbose)
#     data_type = DataType()
#     data_type.name = "rundata"
#     query = "SELECT * FROM " + str(data_type.name) + \
#       " where hydro_cycle_run_time > 0.006"
#     print("Running Query", query)
#     result = store.sqlquery(query)
#     store.export_csv(result, "pennant_query.csv")
#     store.close()

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
    core = Terminal(run_table_flag=False)
    core.load_module('plugin', "Csv", "reader", filenames = csvpath, table_name = table_name)

    if os.path.exists(dbpath):
        os.remove(dbpath)

    #load data into sqlite db
    core.load_module('backend','Sqlite','back-write', filename=dbpath)
    core.artifact_handler(interaction_type='put')

    # update dsi abstraction using a query to the sqlite db
    query_data = core.artifact_handler(interaction_type='get', query = f"SELECT * FROM {table_name} WHERE hydro_cycle_run_time > 0.006;", dict_return = True)
    core.update_abstraction(table_name, query_data)

    #export to csv
    core.load_module('plugin', "Csv_Writer", "writer", filename = output_csv, table_name = table_name)
    core.transload()