import os 
import sys 
# sys.path.append("../DBGenerator")
sys.path.append("../utils")
# from database import Database
from helpers import *
from extractParticles import *
import sqlite3


if __name__ == "__main__":
    ## output_path: where to save extracted halos and galaxies 
    output_path = "/lus/eagle/projects/CosDiscover/mengjiao/test/"
    ## link your metadata database
    # database = Database("/home/mhan/projects/HACCReader/test.db")
    database_path = "/home/mhan/projects/HACCReader/test.db"
    conn = sqlite3.connect(database_path) 
    ## a SQL query command which select all rows, where halo_rank = 0 (largest) from halos table 
    sql = "SELECT * FROM hacc__halos WHERE halo_rank = 0;"
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    ## a dictionary of all rows that halo_ranks = 0
    # rows = database.queryTable(sql)
    n_rows = len(rows)
    print("n_rows", n_rows)
    for r, row in enumerate(rows):
        ## read columns 
        runID = row['run_id']
        ts = row['ts']
        print(r, runID, ts)
        halo_rank = row['halo_rank']
        halo_tag = row['halo_tag']
        halo_count = row['halo_count']
        halo_center_x = row['halo_center_x']
        halo_center_y = row['halo_center_y']
        halo_center_z = row['halo_center_z']
        halo_radius = row['halo_radius']
        ## find the data paths from files table 
        sqlText = f"SELECT bighaloparticles_path, galaxyproperties_path, galaxyparticles_path FROM hacc__files WHERE run_id = {runID} AND ts = {ts};"
        # paths = database.queryTable(sqlText)
        cursor.execute(sqlText)
        paths = cursor.fetchall()
        ## should only have one row, we don't need a for loop here 
        bighaloparticles_path = paths[0]['bighaloparticles_path']
        galaxyproperties_path = paths[0]['galaxyproperties_path']
        galaxyparticles_path = paths[0]['galaxyparticles_path']

        ## using runID find the folder name 
        sqlText = f"SELECT run_path FROM hacc__runs WHERE run_id = {runID};"
        # paths = database.queryTable(sqlText)
        cursor.execute(sqlText)
        paths = cursor.fetchall()
        path = paths[0]['run_path']
        basename = os.path.basename(os.path.normpath(path))
        print(basename)
        bighaloparticles_path = os.path.join(path, bighaloparticles_path)
        galaxyproperties_path = os.path.join(path, galaxyproperties_path)
        galaxyparticles_path = os.path.join(path, galaxyparticles_path)
        ## extract bighalo particles 
        vars = ['x', 'y', 'z', 'phi', 'fof_halo_tag', 'vx', 'vy', 'vz']
        cur_output_path = os.path.join(output_path, basename, str(ts))
        print(cur_output_path)
        create_directory(cur_output_path)
        output_halo_filename = os.path.join(cur_output_path, "halo_" + str(halo_rank))
        [x, y, z, attributes] = extractFromBighaloparticlesByRegion(bighaloparticles_path, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
        # [x, y, z, attributes] = extractFromBighaloparticles(bighaloparticles_path, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars)
        if(len(x) != 0):
            saveToVTK(output_halo_filename, x, y, z, attributes)
