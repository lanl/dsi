#!/usr/bin/env python3

"""
Parses the slurm output from PENNANT runs and creates a csv file
"""

import argparse
import sys
import re
import glob

def main():
    testname = "leblanc"

    """ The data is parsed from all of the .out files in the current directory """
    for slurmoutput in glob.glob('*.out'):
        with open(slurmoutput, 'r') as slurmout:
            data = {}
            data['testname'] = testname
            for line in slurmout:
                if "Running PENNANT" in line:
                    match = re.match(r'Running PENNANT (.*)', line)
                    version = match.group(1)
                    data['version'] = version
                elif "MPI PE(s)" in line:
                    match = re.match(r'Running on (\d+) MPI PE\(s\)', line)
                    pes = match.group(1)
                    data['pes'] = int(pes)
                elif "thread(s)" in line:
                    match = re.match(r'Running on (\d+) thread\(s\).*', line)
                    threads = match.group(1)
                    data['threads'] = int(threads)
                elif "total energy  =" in line:
                    match = re.match(r'.*total energy  =\s+([^\s]+)', line)
                    energy = match.group(1)
                    data['total_energy'] = float(energy)
                elif "internal =" in line and "kinetic =" in line:
                    match = re.match(r'.*internal =\s+([^\s]+), kinetic =\s+([^\s\)]+).*', line)
                    internal_energy = match.group(1)
                    kinetic_energy = match.group(2)
                    data['internal_energy'] = float(internal_energy)
                    data['kinetic_energy'] = float(kinetic_energy)
                elif "time  =" in line:
                    match = re.match(r'.*time  =\s+([^\s,]+),.*', line)
                    time = match.group(1)
                    data['time'] = float(time)
                elif "hydro cycle run time=" in line:
                    match = re.match(r'.*hydro cycle run time=\s+([^\s]+).*', line)
                    hydro_cycle_run_time = match.group(1)
                    data['hydro_cycle_run_time'] = float(hydro_cycle_run_time)

        """ The csv file is created and written to disk """
        with open('pennant_' + testname + '.csv', 'a+') as pennant_out:
            header = ""
            row = ""
            for key, val in data.items():
                header += key + ","
                row += str(val) + ","

            header = header.rstrip(',')
            row = row.rstrip(',')

            if pennant_out.tell() == 0:
                pennant_out.write(header + "\n")

            pennant_out.write(row + "\n")

if __name__ == '__main__':
    main()


