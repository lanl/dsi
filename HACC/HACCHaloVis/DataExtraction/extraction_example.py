import os 
import sys 
sys.path.append("../DBGenerator")
sys.path.append("../utils")
from database import Database
from helpers import *
from extractParticles import *


if __name__ == "__main__":
    ## output_path: where to save extracted halos and galaxies 
    output_path = "/lus/eagle/projects/CosDiscover/mengjiao/halo_galaxy_particles_run_626/"
    ## link your metadata database
    database = Database("../databases/halo_galaxy_largest5_allTS.db")
    ## a SQL query command which select all rows, where halo_rank = 0 (largest) from halos table 
    sql = "SELECT * FROM halos WHERE halo_rank = 0;"
    ## a dictionary of all rows that halo_ranks = 0
    rows = database.queryTable(sql)
    n_rows = len(rows)
    for r, row in enumerate(rows):
        ## read columns 
        runID = row['runID']
        ts = row['ts']
        halo_rank = row['halo_rank']
        halo_tag = row['halo_tag']
        halo_count = row['halo_count']
        halo_center_x = row['halo_center_x']
        halo_center_y = row['halo_center_y']
        halo_center_z = row['halo_center_z']
        halo_radius = row['halo_radius']
        ## find the data paths from files table 
        sqlText = f"SELECT bighaloparticles_path, galaxyproperties_path, galaxyparticles_path FROM files WHERE runID = {runID} AND ts = {ts};"
        paths = database.queryTable(sqlText)
        ## should only have one row, we don't need a for loop here 
        bighaloparticles_path = paths['bighaloparticles_path']
        galaxyproperties_path = paths['galaxyproperties_path']
        galaxyparticles_path = paths['galaxyparticles_path']

        ## using runID find the folder name 
        sqlText = f"SELECT PATH FROM runs WHERE runID = {runID};"
        paths = database.queryTable(sqlText)
        path = paths['PATH']
        basename = os.path.basename(os.path.normpath(path))
        bighaloparticles_path = os.path.join(path, bighaloparticles_path)
        galaxyproperties_path = os.path.join(path, galaxyproperties_path)
        galaxyparticles_path = os.path.join(path, galaxyparticles_path)
        ## extract bighalo particles 
        vars = ['x', 'y', 'z', 'phi', 'fof_halo_tag', 'vx', 'vy', 'vz']
        output_path = os.path.join(output_path, basename, str(ts))
        create_directory(output_path)
        output_halo_filename = os.path.join(output_path, "halo_" + str(halo_rank))
        [x, y, z, attributes] = extractFromBighaloparticlesByRegion(bighaloparticles_path, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
        # [x, y, z, attributes] = extractFromBighaloparticles(bighaloparticles_path, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
        if(len(x) != 0):
            saveToVTK(output_halo_filename, x, y, z, attributes)
