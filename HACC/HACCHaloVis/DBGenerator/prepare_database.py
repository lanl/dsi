import os
import numpy as np
import time
import sys 
sys.path.append("../utils")
import argparse
from helpers import get_time_steps
from database import Database
from readHalos import readHalos


def main(suite_paths, database_save_path, n_halos = 5, n_ts = 113, n_runs = 1):
    ## find time steps available for haloproperties and full resolution data
    ## in the simulations, haloproperties are saved more frequently than full resolution data  
    [haloproperties_ts, full_res_ts] = get_time_steps(os.path.join(suite_paths[0], 'VKIN_10000_EPS_2.440'))
    haloproperties_ts = haloproperties_ts[0:n_ts]
    
    # create a database object
    database = Database(database_save_path) 
    
    ## Create one table with table name 'runs' with [runID, VKIN, EPS, FILE] 
    runs_table_name = "runs"
    table_headers = ["runID", 'VKIN', 'EPS', "PATH"]
    header_types = ['INT', 'FLOAT', 'FLOAT', 'TEXT']
    primary_keys = ['runID']
    database.createTableDefault(runs_table_name, table_headers, header_types, primary_keys)
    ## Create another table with table name 'files' with [key, runID, ts, full_snapshot_path, haloproperties_path, bighaloparticles_path, galaxyproperties_path, galaxyparticles_path]
    ## The primary keys = [key]
    ## runID is a foreign key to the runID in 'runs' table
    files_table_name = 'files'
    table_headers = ['key', 'runID', 'ts', 'full_snapshot_path', 'haloproperties_path', 'bighaloparticles_path', 'galaxyproperties_path', 'galaxyparticles_path']
    header_types = ['INT', 'INT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT']
    primary_keys = ['key']
    database.createTableDefault(files_table_name, table_headers, header_types, primary_keys, [['runID', 'runs']])
    
    ## Create another table with table name 'halos' with [key, runID, ts, halo_rank, halo_tag, halo_count, halo_center_x, halo_center_y, halo_center_z, halo_radius]
    ## The primary keys = [key]
    ## runID is a foreign key to the runID in 'runs' table
    ## ts is a foreign key to the 'files' table
    halo_table_name = 'halos'
    halo_table_headers = ['key', 'runID', 'ts', 'halo_rank', 'halo_tag', 'halo_count', 'halo_center_x', 'halo_center_y', 'halo_center_z', 'halo_radius']
    halo_header_types = ['INT', 'INT', 'INT', 'INT', 'INT', 'INT', "FLOAT", "FLOAT", "FLOAT", "FLOAT"]
    # , 'FLOAT', 'FLOAT', 'FLOAT'
    halo_primary_keys = ['key']
    database.createTableDefault(halo_table_name, halo_table_headers, halo_header_types, halo_primary_keys, [['runID', 'runs'], ['ts', 'files']])
    
    ## initialize table values 
    runs_table_values = [None] * n_runs
    runID_index = 0
    files_table_values = [None] * (n_runs * n_ts) 
    halo_table_values = [None] * (n_runs * n_ts * n_halos)
    halo_index = 0
    vars = ["fof_halo_tag", "fof_halo_count", "fof_halo_center_x", "fof_halo_center_y", "fof_halo_center_z", "sod_halo_radius"]
    filenames = []
    for suite_path in suite_paths:
        ## find all ensembles folder 
        current_filenames = [os.path.join(suite_path, f) for f in os.listdir(suite_path) if f.startswith('VKIN')]
        filenames.append(current_filenames)

    ## only select n_runs ensembles 
    filenames = filenames[0:n_runs]
    for f, filename in enumerate(filenames):
        print("filename", filename)
        basename = os.path.basename(os.path.normpath(filename))
        splits = basename.split('_')
        temp = {"runID": runID_index, 
                "VKIN": splits[1], "EPS": splits[3], "PATH": filename}
        runs_table_values[f] = temp
        runID_index += 1
        
        for t, halo_ts in enumerate(haloproperties_ts):
            # print("halo_ts", halo_ts)
            index = f * n_ts + t
            full_snapshot_path = ""
            if (halo_ts in full_res_ts):
                ## read num_elems and num_variables from genericIO file 
                full_snapshot_path = 'output/full_snapshots/step_' + str(halo_ts) + '/m000p.full.mpicosmo.' + str(halo_ts)
            haloproperties_path = 'analysis/haloproperties/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.haloproperties'
            bighaloparticles_path = 'analysis/bighaloparticles/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.bighaloparticles'
            galaxyproperties_path = 'analysis/galaxyproperties/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.galaxyproperties'
            galaxyparticles_path = 'analysis/galaxyparticles/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.galaxyparticles'
            # start_time = time.perf_counter()
            halo_values = readHalos(os.path.join(filename, haloproperties_path), vars, n_halos)
            # end_time = time.perf_counter()
            # print()
            temp = {"key": index, "runID": runID_index, "ts": halo_ts, \
                    "full_snapshot_path": full_snapshot_path, \
                    "haloproperties_path": haloproperties_path, "bighaloparticles_path": bighaloparticles_path,\
                    "galaxyproperties_path": galaxyproperties_path, "galaxyparticles_path": galaxyparticles_path}
            files_table_values[index] = temp
            ## parse halo values into halo table
            for h in range(n_halos):
                halo_index = f  * (n_halos * n_ts) + t * n_halos + h
                halo_temp = {'key': halo_index, 'runID': runID_index, 'ts': halo_ts, \
                            'halo_rank': h,  'halo_tag': int(halo_values[0][h]), 'halo_count': int(halo_values[1][h]), \
                            'halo_center_x': float(halo_values[2][h]), 'halo_center_y': float(halo_values[3][h]), 'halo_center_z': float(halo_values[4][h]), 'halo_radius': float(halo_values[5][h])}
                halo_table_values[halo_index] = halo_temp 
    
    ## insert values to tables 
    database.insertValueToTable(runs_table_name, runs_table_values)
    database.insertValueToTable(files_table_name, files_table_values)  
    database.insertValueToTable(halo_table_name, halo_table_values)  
    
if __name__ == "__main__":
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser(description='DBGenerator')
    parser.add_argument("--suite_paths", nargs='+', help = 'A list of suite paths')
    parser.add_argument("--output_db_path", type=str, help="path to save database, file extension with .db")

    args = parser.parse_args()
    
    suite_paths = args.suite_paths #["/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM/", "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM_2/"]
    output_db_path = args.output_db_path 
    main(suite_paths, output_db_path)
    
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    
    print("Time taken:", elapsed_time)
    print("All done!\n")
    

