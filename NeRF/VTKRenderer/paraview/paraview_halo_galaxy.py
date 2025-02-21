# trace generated using paraview version 5.13.0
#import paraview
#paraview.compatibility.major = 5
#paraview.compatibility.minor = 13

#### import the simple module from the paraview
from paraview.simple import *
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()
# from math import cos, radians, sin
import os 
import math 
import sqlite3
import time 

start_time = time.perf_counter()
width = 1024 
height = 1024

def get_angles(phi_angle, theta_angle):
    phi = []
    for angle in range(-180, 181, phi_angle):
        phi.append( float(angle) )

    theta = []
    for angle in range(-90, 91, theta_angle):
        theta.append( float(angle) )
    return [phi, theta]

    
[phi, theta] = get_angles(90, 45)

output_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Step_624_HaloGalaxy_5_Test.cdb/"
# halo_galaxy_particles_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/"
halo_galaxy_particles_path = "/lus/eagle/projects/CosDiscover/mengjiao/halo_galaxy_particles_step_624"
# runs = [f for f in os.listdir(halo_galaxy_particles_path) if f.startswith('VKIN')][0:1]

conn = sqlite3.connect("/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/halo_galaxy_5.db")

runs = ['VKIN_3000_EPS_0.760', 'VKIN_3467_EPS_2.067', 'VKIN_3933_EPS_1.320','VKIN_4400_EPS_2.627', \
        'VKIN_4867_EPS_0.387', 'VKIN_5333_EPS_1.693', 'VKIN_5800_EPS_3', 'VKIN_6267_EPS_0.947', \
        'VKIN_6733_EPS_2.253', 'VKIN_7200_EPS_0.200', 'VKIN_7667_EPS_1.507', 'VKIN_8133_EPS_2.813', \
        'VKIN_8600_EPS_0.573', 'VKIN_9067_EPS_1.880', 'VKIN_9533_EPS_1.133', 'VKIN_10000_EPS_2.440'][0:1]

n_halos = 5
for r, run in enumerate(runs):
    print(run)
    run_path = os.path.join(halo_galaxy_particles_path, run, "624")
    # halo_files = [f for f in os.listdir(run_path) if f.startswith('halo')]
    # find runId for current run 
    VKIN = run.split("_")[1]
    EPS = run.split("_")[3]
    query = f"SELECT * FROM runs WHERE VKIN={VKIN} AND EPS ={EPS}"
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    cursor.execute(query)
    answers = cursor.fetchall()
    current_run = None
    for item in answers:
        current_run = {k: item[k] for k in item.keys()}
    runID = current_run['runID']
    
    # for h, halo_file in enumerate(halo_files):
    for h in range(n_halos):
        halo_file = "halo_" + str(h) + ".vtu"
        # tag = halo_file.split("_")[1]
        # rank = halo_file.split("_")[3][0]
        halo_path = os.path.join(run_path, halo_file)
        galaxy_path = os.path.join(run_path, "galaxy_" + str(h) + ".vtu")
        # find halo center
        query = f"SELECT * FROM halos WHERE runID = {runID} AND ts = 624 AND halo_rank = {h}"
        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        cursor.execute(query)
        answers = cursor.fetchall()
        halo_info = None
        for item in answers:
            halo_info = {k: item[k] for k in item.keys()}
        halo_center_x = halo_info['halo_center_x']
        halo_center_y = halo_info['halo_center_y']
        halo_center_z = halo_info['halo_center_z']
        

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
                # position = [origin[0] + r*cos(radians(angle_phi)) * cos(radians(angle_theta)), \
                #             origin[1] + r * sin(radians(angle_phi)) * cos(radians(angle_theta)), \
                #             origin[2]  + r*sin(radians(angle_theta))] 

                # camera.SetPosition(position) 
                camera.Azimuth(60)      # Rotate horizontally by angle_increment
                camera.Elevation(45 * a)  
                Render()
                # save screenshot
                # renderView1.ResetCamera() 
                # renderView1.CameraPosition = [origin[0] + r*cos(radians(angle_phi)) * cos(radians(angle_theta)), \
                #                               origin[1] + r * sin(radians(angle_phi)) * cos(radians(angle_theta)), \
                #                               origin[2] ] 
                image_filename = "phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
                SaveScreenshot(os.path.join(output_path, run + '.cdb', 'ts_624.cdb', 'halo_' + str(h) + '.cdb', image_filename), renderView1, ImageResolution=[width, height])

        # destroy convertToPointCloud1
        Delete(calculator1)
        del calculator1

        Delete(halo_484472722_rank_0vtu)
        del halo_484472722_rank_0vtu
    
        # destroy convertToPointCloud1
        Delete(galaxy_tag_484472722_rank_0vtu)
        del galaxy_tag_484472722_rank_0vtu

end_time = time.perf_counter()    
print("Total time", end_time - start_time)
    
'''   
# #================================================================
# # addendum: following script captures some of the application
# # state to faithfully reproduce the visualization during playback
# #================================================================

# #--------------------------------
# # saving layout sizes for layouts

# # layout/tab size in pixels
# layout1.SetSize(2255, 1162)

# #-----------------------------------
# # saving camera placements for views

# # current camera placement for renderView1
# renderView1.CameraPosition = [62.32024262215301, 20.82007375472998, 257.21466040643463]
# renderView1.CameraFocalPoint = [53.72723842356056, 15.147095680748274, 241.4302870099034]
# renderView1.CameraViewUp = [0.3569592224226302, 0.8002047686513661, -0.4819257637386125]
# renderView1.CameraParallelScale = 5.901997650971939


##--------------------------------------------
## You may need to add some code at the end of this python script depending on your usage, eg:
#
## Render all views to see them appears
# RenderAllViews()
#
## Interact with the view, usefull when running from pvpython
# Interact()
#
## Save a screenshot of the active view
# SaveScreenshot("path/to/screenshot.png")
#
## Save a screenshot of a layout (multiple splitted view)
# SaveScreenshot("path/to/screenshot.png", GetLayout())
#
## Save all "Extractors" from the pipeline browser
# SaveExtracts()
#
## Save a animation of the current active view
# SaveAnimation()
#
## Please refer to the documentation of paraview.simple
## https://www.paraview.org/paraview-docs/latest/python/paraview.simple.html
##--------------------------------------------

'''

        # print(halo_center_x, halo_center_y, halo_center_z)

        # center = renderView1.CameraFocalPoint
        # position = renderView1.CameraPosition
        # radius = ((position[0] - center[0])**2 + 
        #         (position[1] - center[1])**2 + 
        #         (position[2] - center[2])**2) ** 0.5

        # number_of_frames = 36
        # angle_increment = 360 / number_of_frames  # Degrees to rotate per frame
        # elevation_increment = 10 / number_of_frames  # Elevation increment per frame

    # for i in range(number_of_frames):
    #     # angle = 2 * math.pi * i / number_of_frames
    #     # new_x = center[0] + radius * math.cos(angle)
    #     # new_y = center[1] + radius * math.sin(angle)
    #     # new_z = position[2]  # Keep the Z height the same

    #     # Set new camera position
    #     # camera.SetPosition(new_x, new_y, new_z)
    #     # camera.SetViewUp(0, 0, 1)  # Keep the Z-axis as the up direction
    #     # renderView1.CameraPosition = [new_x, new_y, new_z]
    #     # renderView1.CameraViewUp = [0, 0, 1]
    #     # renderView1.CameraFocalPoint = center
    #     camera.Azimuth(angle_increment)      # Rotate horizontally by angle_increment
    #     camera.Elevation(elevation_increment * i)  
        
    #     # Get the camera position and focal point
    #     camera_position = camera.GetPosition()
    #     focal_point = camera.GetFocalPoint()

    #     # Calculate the vector from the focal point to the camera
    #     vector = [camera_position[i] - focal_point[i] for i in range(3)]

    #     # Calculate radius (distance from focal point to camera)
    #     radius = math.sqrt(sum(v**2 for v in vector))

    #     # Azimuth: Angle in the XY plane relative to the X-axis
    #     azimuth = math.degrees(math.atan2(vector[1], vector[0]))

    #     # Elevation: Angle above the XY plane
    #     elevation = math.degrees(math.asin(vector[2] / radius))

    #     # Render the view
    #     Render()
    #     image_filename = "phi_" + str(azimuth) + '_theta_' + str(elevation) + '.png'
    #     SaveScreenshot(output_path + image_filename, renderView1, ImageResolution=[width, height])