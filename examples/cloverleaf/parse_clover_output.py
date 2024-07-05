#!/usr/bin/env python3

"""
Parses the output from CloverLeaf runs and creates a csv file
"""

import argparse
import sys
import re
import glob
import git

def get_repo_and_name_from_url(url: str):
    last_colon_index = url.rfind(":")
    last_suffix_index = url.rfind(".git")
    if last_suffix_index < 0:
        last_suffix_index = len(url)

    if last_colon_index < 0 or last_suffix_index <= last_colon_index:
        raise Exception("Badly formatted url {}".format(url))

    return url[last_colon_index + 1:last_suffix_index]

def getCurrentCommitHash(gitdir):
    repo = git.Repo(gitdir)
    # print(repo.head.object.committer)
    # print(repo.head.object.committed_datetime.strftime("%Y-%m-%d"))
    # print(get_repo_and_name_from_url(repo.remotes.origin.url))
    return repo



def main():
    """ A testname argument is required """
    parser = argparse.ArgumentParser()
    parser.add_argument('--testname', help='the test name')
    parser.add_argument('--gitdir', help='the git directory')
    args = parser.parse_args()
    # testname = "temp_test"
    testname = args.testname
    git_repo = getCurrentCommitHash(args.gitdir)
    if testname is None:
        parser.print_help()
        sys.exit(0)

    data = {}
    data['testname'] = testname
    clover_output = ""
    if git_repo:
        data['git_hash'] = git_repo.head.object.hexsha
        data['git_committer'] = git_repo.head.object.committer
        data['git_committed_date'] = git_repo.head.object.committed_datetime.strftime("%Y-%m-%d")
        data['git_repo_name'] = get_repo_and_name_from_url(git_repo.remotes.origin.url)
        clover_output = "clover_" + git_repo.head.object.hexsha[:7]  +".out"
        print(clover_output)
    else:
        raise Exception("Git repo not found")
        
    with open(clover_output, 'r') as slurmout:
        for line in slurmout:
            if "Clover Version" in line:
                match = re.match(r'Clover Version\s+(\d+.\d+)', line)
                version = match.group(1)
                data['version'] = version
            elif "Task Count" in line:
                match = re.match(r'\s+Task Count\s+(\d+)', line)
                version = match.group(1)
                data['Task Count'] = version
            elif "Thread Count" in line:
                match = re.match(r'\s+Thread Count:\s+(\d+)', line)
                version = match.group(1)
                data['Thread Count'] = version
            elif "=" in line:
                # reading input data
                match = re.match(r'\s+(\w+)=(\d+.?\d+)', line)
                if match:
                    pro_key = match.group(1)
                    pro_value = match.group(2)
                    data[pro_key] = pro_value
            else:
                # reading profiler output
                match = re.match(r'(\w+(\s?\w+)*)\s+:\s+(\d+.\d+)\s+(\d+.\d+)', line)
                if match:
                    pro_key = match.group(1)
                    pro_value = match.group(3)
                    data[pro_key] = pro_value
    # print(data)
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

if __name__ == '__main__':
    main()


