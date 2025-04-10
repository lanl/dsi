import os 
import math 
import sqlite3
import time 
import sys 
sys.path.append('/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/utils')
from cdb import *
from helpers import *

import vtk 
from render import render_halos, set_camera_position_spherical

start_time = time.perf_counter()

# image size 
width = 1024 
height = 1024
# generate camera angles 
aPhi = 90
aTheta = 45  
[phi, theta] = get_angles(aPhi, aTheta)

# path to save the cinema database 
output_path = "/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/outputs/test_vtk.cdb" 

## create a cinema database 
create_directory(output_path)
designCDB = cdb(output_path)
designCDB.initialize()

# path to halos and galaxies data 
halo_galaxy_particles_path = "/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/data/test"
# find all run folders [e.g. VKIN_a_EPS_b]
runs = [f for f in os.listdir(halo_galaxy_particles_path) if f.startswith('FSN')]

# Load the metadata database to retrieve halo properties 
# database = Database("/home/mhan/projects/hacc_halo_vis/databases/halo_galaxy_5.db")
database_path = "/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/data/hacc.db"
conn = sqlite3.connect(database_path) 
conn.row_factory = sqlite3.Row

# only create visualizations for the last time step
timesteps = [624]

# loop for each run 
for r, run in enumerate(runs):
    print(run)
    run_path = os.path.join(halo_galaxy_particles_path, run)
    # find simulation parameter for current run 
    FSN = run.split("_")[1]
    VEL = run.split("_")[3]
    ## create cdb folder to runs
    runCDB_path = os.path.join(output_path, run + ".cdb")
    check = create_directory(runCDB_path)
    runCDB = cdb(runCDB_path)
    if(not check):
        runCDB.initialize()
    else:
        runCDB.read_data_from_file()
    ## read rows from metadata database for current run 
    sqlText = f"SELECT * FROM hacc__runs WHERE FSN={FSN} and VEL = {VEL}"
    # print(FSN, VEL)
    # current_run = database.query(sqlText)
    cursor = conn.cursor()
    cursor.execute(sqlText)
    current_run = cursor.fetchall()
    ## find run_id, then use run_id to find halos in hacc__halos table 
    runID = current_run[0]['run_id']
    print("runId", runID)
    for t, ts in enumerate(timesteps):
        ## current time step path 
        ts_path = os.path.join(halo_galaxy_particles_path, run, str(ts))
        # file all halo_h.vtu in current time step folder 
        halo_files = sorted([f for f in os.listdir(ts_path) if f.startswith('halo')])
        ## create cdb folder for current ts 
        tsCDB_path = os.path.join(runCDB_path, "ts_" + str(ts) + '.cdb')
        check = create_directory(tsCDB_path)
        tsCDB = cdb(tsCDB_path)
        if(not check):
            tsCDB.initialize()
        else:
            tsCDB.read_data_from_file()

        for h, halo_file in enumerate(halo_files):
            rank = h
            halo_path = os.path.join(ts_path, halo_file)
            galaxy_path = os.path.join(ts_path, "galaxy_" + str(rank) + ".vtu")
            # find halo center
            sqlText = f"SELECT * FROM hacc__halos WHERE run_id = {runID} AND ts = 624 AND halo_rank = {rank}"
            # halo_info = database.query(sqlText)
            cursor = conn.cursor()
            cursor.execute(sqlText)
            halo_info = cursor.fetchall()
            # get halo_center
            halo_center_x = halo_info[0]['halo_center_x']
            halo_center_y = halo_info[0]['halo_center_y']
            halo_center_z = halo_info[0]['halo_center_z']
            halo_tag = halo_info[0]['halo_tag']
            halo_count = halo_info[0]['halo_count']
            # create halo cdb database
            haloCDB_path = os.path.join(tsCDB_path, "halo_" + str(h) + '.cdb')
            check = create_directory(haloCDB_path)
            haloCDB = cdb(haloCDB_path)
            if(not check):
                haloCDB.initialize()
            else:
                haloCDB.read_data_from_file()
        
            '''
            VTK Rendering 
            '''

            cur_output_path = os.path.join(output_path, run + '.cdb', 'ts_624.cdb', 'halo_' + str(rank) + '.cdb')
            renderer, render_window = render_halos(halo_path, width, height, theta, phi, [halo_center_x, halo_center_y, halo_center_z], cur_output_path)
            camera = renderer.GetActiveCamera()
            # print(center)
            # print(camera.GetPosition())
            camera.SetPosition(halo_center_x + 8, halo_center_y + 8, halo_center_z + 8)
            camera.SetFocalPoint(halo_center_x, halo_center_y, halo_center_z) 
            camera.SetViewUp(0, 0, 1)
            for angle_phi in phi:
                for a, angle_theta in enumerate(theta):
                    # set_camera_position_spherical(camera, angle_theta, angle_phi, 5, center)
                    # print(camera.GetPosition())
                    # Set azimuth and elevation
                    camera.Azimuth(90)  # Rotate 30 degrees around the view up vector
                    camera.Elevation(45)  # Rotate 20 degrees around the horizontal axis

                    # Ensure the view up vector is orthogonal to the view plane normal
                    camera.OrthogonalizeViewUp()
        #         # Capture image
                    render_window.Render()
                    window_to_image_filter = vtk.vtkWindowToImageFilter()
                    window_to_image_filter.SetInputBufferTypeToRGBA()
                    window_to_image_filter.SetInput(render_window)
                    window_to_image_filter.SetScale(1)  # No scaling
                    window_to_image_filter.Update()
                    writer = vtk.vtkPNGWriter()
                    image_name = "phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
                    image_name = os.path.join(cur_output_path, image_name)
                    # image_name = "./test.png"
                    writer.SetFileName(image_name)
                    writer.SetInputConnection(window_to_image_filter.GetOutputPort())
                    writer.Write()
            
                    ## write cinema database entry
                    
                    haloCDB.add_entry({'phi': str(angle_phi), 'theta': str(angle_theta), 'FILE': image_name}) 
                    image_name = "halo_" + str(rank) + ".cdb/" + image_name
                    tsCDB.add_entry({"halo_rank": rank, "halo_tag": halo_tag, "halo_count": halo_count, 'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    image_name = "ts_" + str(ts) + ".cdb/" + image_name
                    runCDB.add_entry({"ts": str(ts), "halo_rank": rank, "halo_tag": halo_tag, "halo_count": halo_count,  "halo_center_x": halo_center_x, "halo_center_y": halo_center_y, "halo_center_z": halo_center_z, 'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    image_name = run + ".cdb/" + image_name
                    # designCDB.add_entry({"runID": str(runID), "FSN": FSN, "VEL": VEL, "TEXP": TEXP, "BETA": BETA, "SEED": SEED, \
                    #                         "ts": str(ts), "halo_rank": halo_rank, "halo_tag": halo_tag, "halo_count": halo_count, \
                    #                         'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    designCDB.add_entry({"runID": str(runID), "FSN": FSN, "VEL": VEL, \
                                            "ts": str(ts), "halo_rank": rank, "halo_tag": halo_tag, "halo_count": halo_count, \
                                                "halo_center_x": halo_center_x, "halo_center_y": halo_center_y, "halo_center_z": halo_center_z, \
                                            'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})        
        haloCDB.finalize()
    tsCDB.finalize()
runCDB.finalize()
designCDB.finalize()

end_time = time.perf_counter()
print("total time", end_time - start_time)