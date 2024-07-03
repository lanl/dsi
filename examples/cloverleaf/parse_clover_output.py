#!/usr/bin/env python3

"""
Parses the output from CloverLeaf runs and creates a csv file
"""

import argparse
import sys
import re
import glob

def main():
    """ A testname argument is required """
    parser = argparse.ArgumentParser()
    parser.add_argument('--testname', help='the test name')
    args = parser.parse_args()
    # testname = "temp_test"
    testname = args.testname
    if testname is None:
        parser.print_help()
        sys.exit(0)


    """ The data is parsed from all of the .out files in the current directory """
    for slurmoutput in glob.glob('*.out'):
        with open(slurmoutput, 'r') as slurmout:
            data = {}
            data['testname'] = testname
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


