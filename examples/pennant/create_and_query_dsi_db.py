#!/usr/bin/env python3

"""
This script reads in the csv file created from parse_slurm_output.py.
Then it creates a DSI db from the csv file and performs a query.
"""

import argparse
import sys
from dsi.backends.sqlite import Sqlite, DataType

isVerbose = True

"""
Creates the DSI db from the csv file
"""

def import_pennant_data(test_name):
    csvpath = 'pennant_' + test_name + '.csv'
    dbpath = 'pennant_' + test_name + '.db'
    store = Sqlite(dbpath)
    store.put_artifacts_csv(csvpath, "rundata", isVerbose=isVerbose)
    store.close()
    # No error implies success

"""
Performs a sample query on the DSI db
"""
def test_artifact_query(test_name):
    dbpath = "pennant_" + test_name + ".db"
    store = Sqlite(dbpath)
    _ = store.get_artifact_list(isVerbose=isVerbose)
    data_type = DataType()
    data_type.name = "rundata"
    query = "SELECT * FROM " + str(data_type.name) + \
      " where hydro_cycle_run_time > 1.0"
    print("Running Query", query)
    result = store.sqlquery(query)
    store.export_csv(result, "pennant_query.csv")
    store.close()

if __name__ == "__main__":
    """ The testname argument is required """
    parser = argparse.ArgumentParser()
    parser.add_argument('--testname', help='the test name')
    args = parser.parse_args()
    test_name = args.testname
    if test_name is None:
        parser.print_help()
        sys.exit(0)
    
    import_pennant_data(test_name)
    test_artifact_query(test_name)

