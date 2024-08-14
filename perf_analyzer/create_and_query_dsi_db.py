#!/usr/bin/env python3

"""
This script reads in the csv file created from parse_slurm_output.py.
Then it creates a DSI db from the csv file and performs a query.
"""

import argparse
import sys
from dsi.backends.sqlite import Sqlite, DataType
from dsi.plugins.collection_reader import Dict
isVerbose = True


def test_dsi_dict():
    testDict = {"one":1, "two": "number two", "three":"three" }
    dsi_dict = Dict(testDict)
    print("--")
    print(dsi_dict.collections)
    print(dsi_dict.base_dict)
    print(dsi_dict.output_collector)
    # dsi_dict.add_rows()
    print("==>")
    print(dsi_dict.collections)
    print(dsi_dict.base_dict)
    print(dsi_dict.output_collector)
    # dsi_dict.collections.clear()
    dsi_dict.collections.append({"one":15, "three":4, "four":44 })
    # dsi_dict.base_dict["four"] = "fort minor"
    dsi_dict.add_rows()
    print("~~>")
    print(dsi_dict.collections)
    print(dsi_dict.base_dict)
    print(dsi_dict.output_collector)

"""
Creates the DSI db from the csv file
"""

def import_cloverleaf_data(test_name):
    csvpath = 'clover_' + test_name + '.csv'
    dbpath = 'clover_' + test_name + '.db'
    store = Sqlite(dbpath)
    store.put_artifacts_csv(csvpath, "rundata", isVerbose=isVerbose)
    store.close()
    # No error implies success

"""
Performs a sample query on the DSI db
"""
def test_artifact_query(test_name):
    dbpath = "clover_" + test_name + ".db"
    store = Sqlite(dbpath)
    _ = store.get_artifact_list(isVerbose=isVerbose)
    data_type = DataType()
    data_type.name = "rundata"
    query = "SELECT * FROM " + str(data_type.name) + " WHERE Viscosity > 0.1"
    print("Running Query", query)
    result = store.sqlquery(query)
    store.export_csv_query(query, "clover_query.csv")
    store.close()

if __name__ == "__main__":
    # """ The testname argument is required """
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--testname', help='the test name')
    # args = parser.parse_args()
    # test_name = args.testname
    # if test_name is None:
    #     parser.print_help()
    #     sys.exit(0)
    
    # # import_cloverleaf_data(test_name)
    # test_artifact_query(test_name)
    test_dsi_dict()
