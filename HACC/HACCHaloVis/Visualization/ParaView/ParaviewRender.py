# trace generated using paraview version 5.13.0
#import paraview
#paraview.compatibility.major = 5
#paraview.compatibility.minor = 13

#### import the simple module from the paraview
from paraview.simple import *
from vtkmodules.vtkCommonCore import vtkLogger
vtkLogger.SetStderrVerbosity(vtkLogger.VERBOSITY_OFF)
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()

# from math import cos, radians, sin
import os 
import math 
import sqlite3
import time 
import sys 
# sys.path.append('/Users/mhan/Desktop/HACCHaloVis/DBGenerator/')  
sys.path.append('/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/utils')
from helpers import *
# from database import Database 
from cdb import *

start_time = time.perf_counter()

# image size 
width = 1024 
height = 1024
# generate camera angles 
aPhi = 90
aTheta = 45  
[phi, theta] = get_angles(aPhi, aTheta)

# path to save the cinema database 
output_path = "/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/outputs/test.cdb" 
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
            ParaView Rendering 
            '''

            # create a new 'XML Unstructured Grid Reader'
            halo_484472722_rank_0vtu = XMLUnstructuredGridReader(registrationName='halo_484472722_rank_0.vtu', FileName=[halo_path])

            # Properties modified on halo_484472722_rank_0vtu
            halo_484472722_rank_0vtu.TimeArray = 'None'

            # get active view
            renderView1 = GetActiveViewOrCreate('RenderView')

            # show data in view
            halo_484472722_rank_0vtuDisplay = Show(halo_484472722_rank_0vtu, renderView1, 'UnstructuredGridRepresentation')

            # trace defaults for the display properties.
            halo_484472722_rank_0vtuDisplay.Representation = 'Surface'

            # reset view to fit data
            renderView1.ResetCamera(False, 0.9)

            # get the material library
            materialLibrary1 = GetMaterialLibrary()

            # update the view to ensure updated data information
            renderView1.Update()

            # change representation type
            halo_484472722_rank_0vtuDisplay.SetRepresentationType('Points')

            # create a new 'Calculator'
            calculator1 = Calculator(registrationName='Calculator1', Input=halo_484472722_rank_0vtu)

            # Properties modified on calculator1
            calculator1.Function = 'sqrt(vx*vx+vy*vy+vz*vz)'

            # show data in view
            calculator1Display = Show(calculator1, renderView1, 'UnstructuredGridRepresentation')

            # trace defaults for the display properties.
            calculator1Display.Representation = 'Surface'

            # hide data in view
            Hide(halo_484472722_rank_0vtu, renderView1)

            # show color bar/color legend
            calculator1Display.SetScalarBarVisibility(renderView1, True)

            # update the view to ensure updated data information
            renderView1.Update()

            # get color transfer function/color map for 'Result'
            resultLUT = GetColorTransferFunction('Result')

            # get opacity transfer function/opacity map for 'Result'
            resultPWF = GetOpacityTransferFunction('Result')

            # get 2D transfer function for 'Result'
            resultTF2D = GetTransferFunction2D('Result')

            # change representation type
            calculator1Display.SetRepresentationType('Points')

            # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
            resultLUT.ApplyPreset('colormap', True)

            # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
            resultPWF.ApplyPreset('colormap', True)

            # create a new 'XML Unstructured Grid Reader'
            galaxy_tag_484472722_rank_0vtu = XMLUnstructuredGridReader(registrationName='galaxy_tag_484472722_rank_0.vtu', FileName=[galaxy_path])

            # Properties modified on galaxy_tag_484472722_rank_0vtu
            galaxy_tag_484472722_rank_0vtu.TimeArray = 'None'

            # show data in view
            galaxy_tag_484472722_rank_0vtuDisplay = Show(galaxy_tag_484472722_rank_0vtu, renderView1, 'UnstructuredGridRepresentation')

            # trace defaults for the display properties.
            galaxy_tag_484472722_rank_0vtuDisplay.Representation = 'Surface'

            # update the view to ensure updated data information
            renderView1.Update()

            # change representation type
            galaxy_tag_484472722_rank_0vtuDisplay.SetRepresentationType('Points')

            # set scalar coloring
            ColorBy(galaxy_tag_484472722_rank_0vtuDisplay, ('POINTS', 'mass'))

            # rescale color and/or opacity maps used to include current data range
            galaxy_tag_484472722_rank_0vtuDisplay.RescaleTransferFunctionToDataRange(True, False)

            # show color bar/color legend
            galaxy_tag_484472722_rank_0vtuDisplay.SetScalarBarVisibility(renderView1, True)

            # get color transfer function/color map for 'mass'
            massLUT = GetColorTransferFunction('mass')

            # get opacity transfer function/opacity map for 'mass'
            massPWF = GetOpacityTransferFunction('mass')

            # get 2D transfer function for 'mass'
            massTF2D = GetTransferFunction2D('mass')

            # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
            massLUT.ApplyPreset('Blue - Green - Orange', True)

            # Properties modified on galaxy_tag_484472722_rank_0vtuDisplay
            galaxy_tag_484472722_rank_0vtuDisplay.PointSize = 5.0

            # set active source
            SetActiveSource(calculator1)

            # Properties modified on calculator1Display
            calculator1Display.Opacity = 0.3


            # set active source
            SetActiveSource(galaxy_tag_484472722_rank_0vtu)

            # Properties modified on galaxy_tag_484472722_rank_0vtuDisplay
            galaxy_tag_484472722_rank_0vtuDisplay.RenderPointsAsSpheres = 1

            # set active source
            SetActiveSource(calculator1)

            # Properties modified on calculator1Display
            calculator1Display.PointSize = 1.5

            # Properties modified on calculator1Display
            calculator1Display.RenderPointsAsSpheres = 1

            # Properties modified on calculator1Display
            calculator1Display.Ambient = 0.2

            # Properties modified on calculator1Display
            calculator1Display.Luminosity = 0.0

            # Properties modified on calculator1Display
            calculator1Display.RenderLinesAsTubes = 0

            # set active source
            SetActiveSource(galaxy_tag_484472722_rank_0vtu)

            # set active source
            SetActiveSource(calculator1)

            # Properties modified on calculator1
            calculator1.ResultArrayName = 'Velocity_Mag'

            # update the view to ensure updated data information
            renderView1.Update()

            # set scalar coloring
            ColorBy(calculator1Display, ('POINTS', 'Velocity_Mag'))

            # Hide the scalar bar for this color map if no visible data is colored by it.
            HideScalarBarIfNotNeeded(resultLUT, renderView1)

            # rescale color and/or opacity maps used to include current data range
            calculator1Display.RescaleTransferFunctionToDataRange(True, False)

            # show color bar/color legend
            calculator1Display.SetScalarBarVisibility(renderView1, True)

            # get color transfer function/color map for 'Velocity_Mag'
            velocity_MagLUT = GetColorTransferFunction('Velocity_Mag')

            # get opacity transfer function/opacity map for 'Velocity_Mag'
            velocity_MagPWF = GetOpacityTransferFunction('Velocity_Mag')

            # get 2D transfer function for 'Velocity_Mag'
            velocity_MagTF2D = GetTransferFunction2D('Velocity_Mag')

            # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
            velocity_MagLUT.ApplyPreset('colormap', True)

            # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
            velocity_MagPWF.ApplyPreset('colormap', True)

            # get layout
            layout1 = GetLayout()

            # layout/tab size in pixels
            layout1.SetSize(width, height)


            view = GetActiveView()
            camera = GetActiveCamera()


            # current camera placement for renderView1
            # bounds = calculator1.GetDataInformation().DataInformation.GetBounds()
            # halo_center_x = (bounds[1] - bounds[0]) / 2 + bounds[0]
            # halo_center_y = (bounds[3] - bounds[2]) / 2 + bounds[2]
            # halo_center_z = (bounds[5] - bounds[4]) / 2 + bounds[4]
            renderView1.CameraPosition = [halo_center_x - 1, halo_center_y - 1, halo_center_z]
            renderView1.CameraFocalPoint = [halo_center_x, halo_center_y, halo_center_z]
            renderView1.CameraViewUp = [0.0, 0.0, 1.0]
            renderView1.CameraParallelScale = 10
            # Properties modified on renderView1
            renderView1.UseColorPaletteForBackground = 0

            # Properties modified on renderView1
            renderView1.Background = [0.0, 0.0, 0.0]

            for angle_phi in phi:
                for a, angle_theta in enumerate(theta):
                    camera.Azimuth(aPhi)      # Rotate horizontally by angle_increment
                    camera.Elevation(aTheta * a)  
                    Render()
                    # save screenshot
                    image_name = "phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
                    SaveScreenshot(os.path.join(output_path, run + '.cdb', 'ts_624.cdb', 'halo_' + str(rank) + '.cdb', image_name), renderView1, ImageResolution=[width, height])
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
            # destroy convertToPointCloud1
            Delete(calculator1)
            del calculator1

            Delete(halo_484472722_rank_0vtu)
            del halo_484472722_rank_0vtu
        
            # destroy convertToPointCloud1
            Delete(galaxy_tag_484472722_rank_0vtu)
            del galaxy_tag_484472722_rank_0vtu
        haloCDB.finalize()
    tsCDB.finalize()
runCDB.finalize()
designCDB.finalize()

end_time = time.perf_counter()
print("total time", end_time - start_time)