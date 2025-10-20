#!/usr/bin/env python3

"""
Parses the output from CloverLeaf runs and creates a csv file
"""

import argparse
import sys
import re
import glob
import git
import pandas as pd
from dsi.plugins.collection_reader import Dict
from dsi.backends.sqlite import Sqlite, DataType
import json

def get_repo_and_name_from_url(url: str):
    last_colon_index = url.rfind(":")
    last_suffix_index = url.rfind(".git")
    if last_suffix_index < 0:
        last_suffix_index = len(url)

    if last_colon_index < 0 or last_suffix_index <= last_colon_index:
        raise Exception("Badly formatted url {}".format(url))

    return url[last_colon_index + 1:last_suffix_index]

def get_git_repo(gitdir):
    repo = git.Repo(gitdir)
    # print(repo.head.object.committer)
    # print(repo.head.object.committed_datetime.strftime("%Y-%m-%d"))
    # print(get_repo_and_name_from_url(repo.remotes.origin.url))
    return repo

def add_output_to_csv_file(data, testname):
    """ The csv file is created and written to disk """
    with open('clover_' + testname + '.csv', 'a+') as clover_out:
        header = ""
        row = ""
        for key, val in data.items():
            header += key + ","
            row += str(val) + ","

        header = header.rstrip(',')
        row = row.rstrip(',')

        if clover_out.tell() == 0:
            clover_out.write(header + "\n")

        clover_out.write(row + "\n")

def add_non_existing_columns(data, test_name, db_base_dir):
    dbpath = db_base_dir + "/clover_" + test_name + ".db"
    store = Sqlite(dbpath)
    data_type = DataType()
    data_type.name = "TABLENAME"

    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + data_type.name + "'"
    result = store.sqlquery(query)
    if len(result) == 0:
        return
    
    str_query = "PRAGMA table_info( " + data_type.name + " );"
    result = store.sqlquery(str_query)
    art_list = [tup[1] for tup in result]
    for key in data:
        if key not in art_list:
            print("key not exist: ", key)
            str_query = "ALTER TABLE " + data_type.name + " ADD " + key + " VARCHAR DEFAULT None"
            result = store.sqlquery(str_query)
            print(key, " added")
    store.close()
    

def add_output_to_dsi(data, test_name, db_base_dir):
    dbpath = db_base_dir + '/clover_' + test_name + '.db'
    dsi_dict = Dict(data)
    dsi_dict.add_rows()
    # print(dsi_dict.collections)

    add_non_existing_columns(data, test_name, db_base_dir)

    store = Sqlite(dbpath)
    # store.types.name = test_name
    store.put_artifacts(dsi_dict.collections[0], isVerbose=False)
    store.close()


"""
Performs a sample query on the DSI db
"""
def test_artifact_query(test_name, db_base_dir, git_hash):
    dbpath = db_base_dir +  "/clover_" + test_name + ".db"
    store = Sqlite(dbpath)
    _ = store.get_artifact_list(isVerbose=False)
    data_type = DataType()
    data_type.name = "TABLENAME"
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + data_type.name + "'"
    result = store.sqlquery(query)
    if len(result) == 0:
        return False
    query = "SELECT count(*) as cn FROM " + str(data_type.name) + " WHERE git_hash LIKE '" + git_hash + "%'"
    print("Running Query", query)
    result = store.sqlquery(query)
    store.close()
    if len(result) > 0 and result[0][0] > 0:
        print("found")
        return True
    else:
        print("not found")
    return False
    # store.export_csv_query(query, "clover_query.csv")


"""
Performs a sample query on the DSI db
"""
def get_all_db_data(test_name, db_base_dir):
    dbpath = db_base_dir +  "/clover_" + test_name + ".db"
    store = Sqlite(dbpath)
    _ = store.get_artifact_list(isVerbose=False)
    data_type = DataType()
    data_type.name = "TABLENAME"
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + data_type.name + "'"
    result = store.sqlquery(query)
    if len(result) == 0:
        return None
    query = "SELECT * FROM " + str(data_type.name)
    print("Running Query", query)
    result = pd.read_sql(query, store.con)
    # result = store.sqlquery(query)
    store.close()
    return result


def process_keys_for_sqlite(key):
    return key.replace(" ", "_").lower()

def parse_clover_output_file(testname, git_dir):
    git_repo = get_git_repo(git_dir)
    data = {}
    data['testname'] = [testname]
    clover_output = git_dir + "/"
    if git_repo:
        data['git_hash'] = [git_repo.head.object.hexsha]
        data['git_committer'] = [git_repo.head.object.committer.email]
        data['git_committed_date'] = [git_repo.head.object.committed_datetime.strftime("%Y-%m-%d %H:%M:%S")]
        data['git_repo_name'] = [get_repo_and_name_from_url(git_repo.remotes.origin.url)]
        clover_output = clover_output + "clover.out"
        # print(clover_output)
    else:
        raise Exception("Git repo not found")
        
    with open(clover_output, 'r') as slurmout:
        for line in slurmout:
            if "Clover Version" in line:
                match = re.match(r'Clover Version\s+(\d+.\d+)', line)
                version = match.group(1)
                data['version'] = [version]
            elif "Task Count" in line:
                match = re.match(r'\s+Task Count\s+(\d+)', line)
                version = match.group(1)
                data['Task_Count'] = [version]
            elif "Thread Count" in line:
                match = re.match(r'\s+Thread Count:\s+(\d+)', line)
                version = match.group(1)
                data['Thread_Count'] = [version]
            elif "=" in line:
                # reading input data
                match = re.match(r'\s+(\w+)=(\d+.?\d+)', line)
                if match:
                    pro_key = process_keys_for_sqlite(match.group(1))
                    pro_value = match.group(2)
                    data[pro_key] = [pro_value]
            else:
                # reading profiler output
                match = re.match(r'(\w+(\s?\w+)*)\s+:\s+(\d+.\d+)\s+(\d+.\d+)', line)
                if match:
                    pro_key = process_keys_for_sqlite(match.group(1))
                    pro_value = match.group(3)
                    data[pro_key] = [pro_value]
    # print(data)
    return data

def parse_sim_output_file(testname, git_dir):
    return parse_clover_output_file(testname, git_dir)

def parse_tau_output_file(testname, git_dir):
    git_repo = get_git_repo(git_dir)
    data = {}
    data['testname'] = [testname]
    tau_output_file = git_dir + "/" + "tau_results"
    if git_repo:
        data['git_hash'] = [git_repo.head.object.hexsha]
        data['git_committer'] = [git_repo.head.object.committer.email]
        data['git_committed_date'] = [git_repo.head.object.committed_datetime.strftime("%Y-%m-%d %H:%M:%S")]
        data['git_repo_name'] = [get_repo_and_name_from_url(git_repo.remotes.origin.url)]
    else:
        raise Exception("Git repo not found")
    
    do_parse = False
    core_parse = False
    nct_text = ""
    with open(tau_output_file, 'r') as slurmout:
        for line in slurmout:
            core_parse_match = re.match(r'(NODE\ )(\d+)(;CONTEXT\ )(\d+)(;THREAD\ )(\d+)\:', line)
            core_false_match = re.match(r'(USER EVENTS Profile \:).*', line)
            total_match = re.match(r'(FUNCTION SUMMARY \(total\)\:).*', line)
            mean_match = re.match(r'(FUNCTION SUMMARY \(mean\)\:).*', line)

            if core_parse_match:
                core_parse = True
                nct_text = "_" + core_parse_match.group(2)# + "_" + core_parse_match.group(4) + "_" + core_parse_match.group(6)
            elif core_parse and core_false_match:
                core_parse = False
                nct_text = ""

            
            if total_match:
                do_parse = True
                nct_text = ""
            elif mean_match:
                do_parse = False

            if do_parse is True or core_parse is True:
                if "MPI_" in line:
                    print(line)
                    match = re.match(r'\s+(\d+\.\d+)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)\s+([\d,\.]+)\s+(MPI_\w+)\(\)\s+', line)
                    # data[match.group(7) + '_time'] = [match.group(1)]
                    data[match.group(7) + nct_text + '_esec'] = [float(match.group(2))/1000]
                    data[match.group(7) + nct_text + '_isec'] = [float(match.group(3))/1000]
                    # data[match.group(7) + '_calls'] = [match.group(4)]
                    # data[match.group(7) + '_subs'] = [match.group(5)]
                    # data[match.group(7) + '_iusec_p_call'] = [match.group(6)]
    # print(data)
    return data




def main():
    # """ A testname argument is required """
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--testname', help='the test name')
    # parser.add_argument('--gitdir', help='the git directory')
    # args = parser.parse_args()
    # # testname = "temp_test"
    # testname = args.testname
    # if testname is None:
    #     parser.print_help()
    #     sys.exit(0)

    testname = "test"
    gitdir = "/Users/ssakin/projects/CloverLeaf/CloverLeaf_ref/tauProfiles"
    # data = parse_clover_output_file(testname, args.gitdir)
    data = parse_tau_output_file(testname, gitdir)
    # print(data)
    # add_output_to_csv_file(data, testname)
    # add_output_to_dsi(data, testname)

    # test_artifact_query(testname)

if __name__ == '__main__':
    main()


