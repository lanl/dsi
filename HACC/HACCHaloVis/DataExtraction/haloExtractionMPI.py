import os 
import sys 
sys.path.append("../DBGenerator")
sys.path.append("../utils")
from database import Database
from helpers import *
from extractParticles import *
import time
import logging
from datetime import datetime
from mpi4py import MPI
import math

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
database = Database("../databases/halo_galaxy_largest5_allTS.db")

def process_task(task):
    runID = task['runID']
    ts = task['ts']
    halo_rank = task['halo_rank']
    halo_tag = task['halo_tag']
    halo_count = task['halo_count']
    halo_center_x = task['halo_center_x']
    halo_center_y = task['halo_center_y']
    halo_center_z = task['halo_center_z']
    halo_radius = task['halo_radius']
    
    ## find the full res path from files table 
    sqlText = f"SELECT bighaloparticles_path, galaxyproperties_path, galaxyparticles_path FROM files WHERE runID = {runID} AND ts = {ts};"
    answers = database.queryTable(sqlText)
    temp = {k: answers[0][k] for k in answers[0].keys()}
    bighaloparticles_path = temp['bighaloparticles_path']
    galaxyproperties_path = temp['galaxyproperties_path']
    galaxyparticles_path = temp['galaxyparticles_path']
    
    ## find the run folder name 
    sqlText = f"SELECT PATH FROM runs WHERE runID = {runID};"
    answers = database.queryTable(sqlText)
    temp = {k: answers[0][k] for k in answers[0].keys()}
    path = temp['PATH']
    basename = os.path.basename(os.path.normpath(path))
    bighaloparticles_path = os.path.join(path, bighaloparticles_path)
    galaxyproperties_path = os.path.join(path, galaxyproperties_path)
    galaxyparticles_path = os.path.join(path, galaxyparticles_path)
    
    ## extract bighalo particles 
    vars = ['x', 'y', 'z', 'phi', 'fof_halo_tag', 'vx', 'vy', 'vz']
    output_path = os.path.join("/lus/eagle/projects/CosDiscover/mengjiao/halo_galaxy_particles_run_6267/", basename, str(ts))
    create_directory(output_path)
    output_halo_filename = os.path.join(output_path, "halo_" + str(halo_rank))
    [x, y, z, attributes] = extractFromBighaloparticlesByRegion(bighaloparticles_path, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
    # [x, y, z, attributes] = extractFromBighaloparticles(bighaloparticles_path, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
    if(len(x) != 0):
        saveToVTK(output_halo_filename, x, y, z, attributes)
        
    # ## extract galaxy particles
    # vars = ['gal_tag', 'gal_mass', 'fof_halo_tag']
    # # , 'fof_halo_count', 'fof_halo_galaxy_count','fof_halo_center_x', 'fof_halo_center_y', 'fof_halo_center_z']
    # output_galaxy_filename = os.path.join(output_path, "galaxy_ " + str(halo_rank))
    # [x, y, z, attributes] = extractFromGalaxyparticles(galaxyproperties_path, galaxyparticles_path, vars, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius)
    # if(len(x) != 0):
    #     saveToVTK(output_galaxy_filename, x, y, z, attributes)
    
if rank == 0:
    start_time = time.perf_counter()
    ## read halos table 
    sql = "SELECT * FROM halos WHERE halo_rank = 0;"
    answers = database.queryTable(sql)
    tasks = []
    for item in answers:
        temp = {k: item[k] for k in item.keys()}
        tasks.append(temp)
    # tasks = tasks[0:80]
    n_tasks = len(tasks)
    print("num tasks", n_tasks) 
    sys.stdout.flush()
    
    
    # Distribute tasks using round-robin
    task_chunks = {i: [] for i in range(1, size)}  # Create empty lists for workers
    for i, task in enumerate(tasks):
        worker_rank = (i % (size - 1)) + 1  # Assign tasks round-robin to workers
        task_chunks[worker_rank].append(task)
    # Send tasks to each worker
    for worker_rank in range(1, size):
        comm.send(task_chunks[worker_rank], dest=worker_rank, tag=0)
        # print(f"Rank 0 sent {len(task_chunks[worker_rank])} tasks to rank {worker_rank}.")        
    
    '''
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
    '''
else:
    # Worker ranks receive tasks from rank 0
    chunk = comm.recv(source=0, tag=0)
    # print(f"Rank {rank} received {len(chunk)} tasks.") 
    sys.stdout.flush()
    for t, task in enumerate(chunk):
        process_task(task)
        
comm.Barrier()
if rank == 0:
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time taken:", elapsed_time)
    print("All done!\n") 
MPI.Finalize() 
