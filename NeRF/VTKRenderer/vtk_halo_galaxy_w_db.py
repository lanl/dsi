import sys
import os 
from database import Database

database = Database("/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/halo_galaxy_5.db")

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
    output_path = os.path.join("/lus/eagle/projects/CosDiscover/mengjiao/halo_galaxy_particles_step_624/", basename, str(ts))
    # create_directory(output_path)
    output_halo_filename = os.path.join(output_path, "halo_" + str(halo_tag) + "_rank_" + str(halo_rank))
    [x, y, z, attributes] = extractFromBighaloparticles(bighaloparticles_path, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius, vars, output_halo_filename)
    
    ## extract galaxy particles
    vars = ['gal_tag', 'fof_halo_tag']
    # , 'fof_halo_count', 'fof_halo_galaxy_count','fof_halo_center_x', 'fof_halo_center_y', 'fof_halo_center_z']
    output_galaxy_filename = os.path.join(output_path, "galaxy_tag_" + str(halo_tag) + "_rank_" + str(halo_rank))
    [x, y, z, attributes] = extractFromGalaxyparticles(galaxyproperties_path, galaxyparticles_path, vars, halo_tag, halo_center_x, halo_center_y, halo_center_z, halo_radius, output_galaxy_filename)
    
    render
    
    
if __name__ == "__main__":
    query = "SELECT * FROM halos WHERE ts = 624;"
    answers = database.queryTable(query)
    tasks = []
    for item in answers:
        temp = {k: item[k] for k in item.keys()}
        tasks.append(temp)
    tasks = tasks[0:10]
    n_tasks = len(tasks)
    print("num tasks", n_tasks) 
    sys.stdout.flush()
    

 