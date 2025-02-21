import os
import numpy as np
import time
import sys 
sys.path.append("../utils")
from helpers import *
from database import Database
from readHalos import readHalos
from mpi4py import MPI
import math

"""Parallel creating database using MPI

"""
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
n_halos = 5
n_ts = 113
n_runs = 1
[haloproperties_ts, full_res_ts] = match_time_steps('/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM/VKIN_10000_EPS_2.440')
vars = ["fof_halo_tag", "fof_halo_count", "fof_halo_center_x", "fof_halo_center_y", "fof_halo_center_z", "sod_halo_radius"]

results = [] # this empty array is used to collect results from all ranks 
     
def process_task(task):
    filename = task['filename']
    f_index = task['f_index']
    t_index = task['t_index']
    halo_ts = task['ts']
    index = f_index * n_ts + t_index
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
    temp = {"key": index, "runID": f_index, "ts": halo_ts, \
            "full_snapshot_path": full_snapshot_path, \
            "haloproperties_path": haloproperties_path, "bighaloparticles_path": bighaloparticles_path,\
            "galaxyproperties_path": galaxyproperties_path, "galaxyparticles_path": galaxyparticles_path}
    # files_table_values[index] = temp
    ## parse halo values into halo table
    halo_table_values = [None] * n_halos
    for h in range(n_halos):
        halo_index = f_index  * (n_halos * n_ts) + t_index * n_halos + h
        halo_temp = {'key': halo_index, 'runID': f_index, 'ts': halo_ts, \
                    'halo_rank': h,  'halo_tag': int(halo_values[0][h]), 'halo_count': int(halo_values[1][h]), \
                    'halo_center_x': float(halo_values[2][h]), 'halo_center_y': float(halo_values[3][h]), 'halo_center_z': float(halo_values[4][h]), 'halo_radius': float(halo_values[5][h])}
        halo_table_values[h] = halo_temp
    return [temp, halo_table_values]
    


suite_paths = ["/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM/", "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM_2/"]
database_save_path =  "../databases/halo_galaxy_largest5_allTS.db"  

if rank == 0:
    start_time = time.perf_counter()
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
    

    ## Create another table with table name 'halos' with [key, runID, ts, halo_rank, halo_tag, halo_count, halo_center_x, halo_center_y, halo_center_z]
    ## The primary keys = [key]
    ## runID is a foreign key to the runID in 'runs' table
    ## ts is a foreign key to the 'files' table
    halo_table_name = 'halos'
    halo_table_headers = ['key', 'runID', 'ts', 'halo_rank', 'halo_tag', 'halo_count', 'halo_center_x', 'halo_center_y', 'halo_center_z', 'halo_radius']
    halo_header_types = ['INT', 'INT', 'INT', 'INT', 'INT', 'INT', "FLOAT", "FLOAT", "FLOAT", "FLOAT"]
    # , 'FLOAT', 'FLOAT', 'FLOAT'
    halo_primary_keys = ['key']
    database.createTableDefault(halo_table_name, halo_table_headers, halo_header_types, halo_primary_keys, [['runID', 'runs'], ['ts', 'files']])
    ## Gather all runs from two suites 
    filenames = []
    for suite_path in suite_paths:
        current_filenames = [os.path.join(suite_path, f) for f in os.listdir(suite_path) if f.startswith('VKIN')]
        filenames += current_filenames
    filenames = filenames[0:n_runs]
    tasks = []
    runs_table_values = []
    for f, filename in enumerate(filenames):
        basename = os.path.basename(os.path.normpath(filename))
        splits = basename.split('_')
        temp = {"runID": f, "VKIN": splits[1], "EPS": splits[3], "PATH": filename}
        runs_table_values.append(temp)
        for t, ts in enumerate(haloproperties_ts):
            temp = {'f_index': f, 'filename': filename, 't_index': t, "ts": ts}
            tasks.append(temp)
    ## first insert run table 
    database.insertValueToTable(runs_table_name, runs_table_values)
    
    n_tasks = len(tasks)
    print("num tasks", n_tasks) 
    sys.stdout.flush()
    chunks = []
    workers = size - 1
    chunk_size = math.ceil(len(tasks) / workers)
    for i in range(1, size):  # Distribute only to worker ranks
        start = (i - 1) * chunk_size
        end = min(start + chunk_size, len(tasks))
        chunk = tasks[start:end]
        comm.send(chunk, dest=i, tag=0)
        # print(f"Rank 0 sent {len(chunk)} tasks to rank {i}.") 
        sys.stdout.flush()
    
    for i in range(1, size):
        result = comm.recv(source=i, tag=1)
        results.extend(result) 

else:
    # Worker ranks receive tasks from rank 0
    chunk = comm.recv(source=0, tag=0)
    # print(f"Rank {rank} received {len(chunk)} tasks.") 
    sys.stdout.flush()
    current_results = []
    for t, task in enumerate(chunk):
        current_result = process_task(task)
        # print(current_result)
        # print(f"Rank {rank} done {t} tasks.") 
        sys.stdout.flush()
        current_results.extend(current_result)
    comm.send(current_results, dest=0, tag=1)   

## wait for all ranks 
comm.Barrier()

if rank == 0:
    # print("All workers have completed their tasks.") 
    # sys.stdout.flush()
    files_table_values = [None] * (n_runs * n_ts) 
    halo_table_values = [None] * (n_runs * n_ts * n_halos)
    for r, result in enumerate(results): 
        if r % 2 == 0: 
            index = result['key']
            files_table_values[index] = result 
        else:
            for halo_value in result:
                halo_index = halo_value['key']
                halo_table_values[halo_index] = halo_value 
    
    database.insertValueToTable(files_table_name, files_table_values)  
    database.insertValueToTable(halo_table_name, halo_table_values) 
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time taken:", elapsed_time)
    print("All done!\n") 
    
MPI.Finalize()        
        
    
# if __name__ == "__main__":
    # start_time = time.perf_counter()
    # # suite_path = "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/128MPC_RUNS_FLAMINGO_DESIGN_3A/"
    # suite_paths = ["/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM/", "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM_2/"]
    # main(suite_paths, "../databases/halo_galaxy_5.db")
