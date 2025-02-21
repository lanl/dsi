#### import the simple module from the paraview
from paraview.simple import *
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()
# from math import cos, radians, sin
import os 
import math 
import sqlite3
import time 
import sys 
sys.path.append("/Users/mhan/Desktop/Examples/VTKRenderer/")
from compute_pose_focal import *

start_time = time.perf_counter()
width = 100 
height = 100

num_images = 100
azimuth_range = 360  # Range of azimuth rotation in degrees
elevation_range = 180  # Range of elevation rotation in degrees
azimuth_step = azimuth_range / num_images  # Azimuth angle increment
elevation_step = elevation_range / num_images  # Elevation angle increment


# Set up the initial camera position
def spherical_to_cartesian(radius, azimuth, elevation):
    azimuth_rad = math.radians(azimuth)
    elevation_rad = math.radians(elevation)
    x = radius * math.cos(elevation_rad) * math.cos(azimuth_rad)
    y = radius * math.cos(elevation_rad) * math.sin(azimuth_rad)
    z = radius * math.sin(elevation_rad)
    return [x, y, z]

# def get_angles(phi_angle, theta_angle):
#     phi = []
#     for angle in range(-180, 181, phi_angle):
#         phi.append( float(angle) )

#     theta = []
#     for angle in range(-90, 91, theta_angle):
#         theta.append( float(angle) )
#     return [phi, theta]

    
# [phi, theta] = get_angles(20, 20)

output_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM_Train/STEP_624_ONE_HALO/"
halo_galaxy_particles_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles_Normalized/"
runs = [f for f in os.listdir(halo_galaxy_particles_path) if f.startswith('VKIN')][0:1]

conn = sqlite3.connect("/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/halo_galaxy_5_624_test.db")

for r, run in enumerate(runs):
    print(run)
    run_path = os.path.join(halo_galaxy_particles_path, run, "624")
    halo_files = sorted([f for f in os.listdir(run_path) if f.startswith('halo')])[0:1] ## pick the largest halo
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
    
    for h, halo_file in enumerate(halo_files):
        # tag = halo_file.split("_")[1]
        rank = halo_file.split("_")[1][0]
        halo_path = os.path.join(run_path, halo_file)
        galaxy_path = os.path.join(run_path, "galaxy_" + str(rank) + ".vtu")
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
        halo_radius = halo_info['halo_radius']

        # create a new 'XML Unstructured Grid Reader'
        halo_2vtu = XMLUnstructuredGridReader(registrationName='halo_2.vtu', FileName=[halo_path])

        # Properties modified on halo_2vtu
        halo_2vtu.TimeArray = 'None'

        # get active view
        renderView1 = GetActiveViewOrCreate('RenderView')

        # show data in view
        halo_2vtuDisplay = Show(halo_2vtu, renderView1, 'UnstructuredGridRepresentation')

        # trace defaults for the display properties.
        halo_2vtuDisplay.Representation = 'Surface'

        # reset view to fit data
        renderView1.ResetCamera(False, 0.9)

        # get the material library
        materialLibrary1 = GetMaterialLibrary()

        # show color bar/color legend
        halo_2vtuDisplay.SetScalarBarVisibility(renderView1, False)

        renderView1.OrientationAxesVisibility = 0
        # update the view to ensure updated data information
        renderView1.Update()

        # get color transfer function/color map for 'v_mag'
        v_magLUT = GetColorTransferFunction('v_mag')

        # get opacity transfer function/opacity map for 'v_mag'
        v_magPWF = GetOpacityTransferFunction('v_mag')
        
                # Rescale transfer function
        v_magLUT.RescaleTransferFunction(0.0, 1.0)

        # Rescale transfer function
        v_magPWF.RescaleTransferFunction(0.0, 1.0)

        # get 2D transfer function for 'v_mag'
        v_magTF2D = GetTransferFunction2D('v_mag')
        
        # get color legend/bar for v_magLUT in view renderView1
        v_magLUTColorBar = GetScalarBar(v_magLUT, renderView1)

        # Properties modified on v_magLUTColorBar
        v_magLUTColorBar.AutoOrient = 0
        v_magLUTColorBar.Orientation = 'Horizontal'

        # change scalar bar placement
        v_magLUTColorBar.Position = [0.645440844009043, 0.0035398230088495575]

        # change scalar bar placement
        v_magLUTColorBar.WindowLocation = 'Any Location'
        v_magLUTColorBar.ScalarBarLength = 0.2003843255463451

        # create a new 'XML Unstructured Grid Reader'
        galaxy_2vtu = XMLUnstructuredGridReader(registrationName='galaxy_2.vtu', FileName=[galaxy_path])

        # Properties modified on galaxy_2vtu
        galaxy_2vtu.TimeArray = 'None'

        # show data in view
        galaxy_2vtuDisplay = Show(galaxy_2vtu, renderView1, 'UnstructuredGridRepresentation')

        # trace defaults for the display properties.
        galaxy_2vtuDisplay.Representation = 'Surface'

        # update the view to ensure updated data information
        renderView1.Update()

        # set scalar coloring
        ColorBy(galaxy_2vtuDisplay, ('POINTS', 'mass'))

        # rescale color and/or opacity maps used to include current data range
        galaxy_2vtuDisplay.RescaleTransferFunctionToDataRange(True, False)

        # show color bar/color legend
        galaxy_2vtuDisplay.SetScalarBarVisibility(renderView1, False)

        # get color transfer function/color map for 'mass'
        massLUT = GetColorTransferFunction('mass')

        # get opacity transfer function/opacity map for 'mass'
        massPWF = GetOpacityTransferFunction('mass')

        # get 2D transfer function for 'mass'
        massTF2D = GetTransferFunction2D('mass')
        
        # get color legend/bar for massLUT in view renderView1
        massLUTColorBar = GetScalarBar(massLUT, renderView1)

        # Properties modified on massLUTColorBar
        massLUTColorBar.AutoOrient = 0
        massLUTColorBar.Orientation = 'Horizontal'
        massLUTColorBar.WindowLocation = 'Lower Left Corner'

        # change scalar bar placement
        # massLUTColorBar.WindowLocation = 'Any Location'
        # massLUTColorBar.Position = [0.024114544084400905, 0.0035398230088495575]
        # massLUTColorBar.ScalarBarLength = 0.19661642803315665

        # set active source
        SetActiveSource(halo_2vtu)

        # set active source
        SetActiveSource(galaxy_2vtu)

        # Properties modified on galaxy_2vtuDisplay
        galaxy_2vtuDisplay.PointSize = 4.0

        # set active source
        SetActiveSource(halo_2vtu)

        # Apply a preset using its name. Note this may not work as expected when presets have duplicate names.
        v_magLUT.ApplyPreset('Fast', True)

        # set active source
        SetActiveSource(galaxy_2vtu)

        # set active source
        SetActiveSource(halo_2vtu)

        # set active source
        SetActiveSource(galaxy_2vtu)

        # Rescale transfer function
        massLUT.RescaleTransferFunction(0.0, 1.0)

        # Rescale transfer function
        massPWF.RescaleTransferFunction(0.0, 1.0)

        # Properties modified on galaxy_2vtuDisplay
        galaxy_2vtuDisplay.RenderPointsAsSpheres = 1

        # Properties modified on galaxy_2vtuDisplay
        galaxy_2vtuDisplay.Ambient = 0.28

        # Properties modified on galaxy_2vtuDisplay
        galaxy_2vtuDisplay.Ambient = 0.3

        # set active source
        SetActiveSource(halo_2vtu)

        # Properties modified on halo_2vtuDisplay
        halo_2vtuDisplay.Opacity = 0.15

        # Rescale transfer function
        v_magLUT.RescaleTransferFunction(0.0001, 1.0)

        # Rescale transfer function
        v_magPWF.RescaleTransferFunction(0.0001, 1.0)

        # convert to log space
        v_magLUT.MapControlPointsToLogSpace()

        # Properties modified on v_magLUT
        v_magLUT.UseLogScale = 1

        # convert from log to linear
        v_magLUT.MapControlPointsToLinearSpace()

        # Properties modified on v_magLUT
        v_magLUT.UseLogScale = 0

        # Properties modified on halo_2vtuDisplay
        halo_2vtuDisplay.PointSize = 1.0

        # Properties modified on halo_2vtuDisplay
        halo_2vtuDisplay.Opacity = 0.3
        # get layout
        layout1 = GetLayout()

        # layout/tab size in pixels
        layout1.SetSize(width, height)


        view = GetActiveView()
        camera = GetActiveCamera()


        # current camera placement for renderView1
        bounds = halo_2vtu.GetDataInformation().DataInformation.GetBounds()
        bound_center_x = (bounds[1] - bounds[0]) / 2 + bounds[0]
        bound_center_y = (bounds[3] - bounds[2]) / 2 + bounds[2]
        bound_center_z = (bounds[5] - bounds[4]) / 2 + bounds[4]
        radius_x = (bounds[1] - bounds[0]) / 2
        radius_y = (bounds[3] - bounds[2]) / 2
        renderView1.CameraPosition = [bound_center_x - 4 * radius_x, bound_center_y - 4 * radius_y, halo_center_z]
        renderView1.CameraFocalPoint = [bound_center_x, bound_center_y, bound_center_z]
        camera_distance = math.dist(renderView1.CameraPosition, renderView1.CameraFocalPoint)
        # renderView1.CameraPosition = [halo_center_x - halo_radius, halo_center_y - halo_radius, halo_center_z]
        # renderView1.CameraFocalPoint = [halo_center_x, halo_center_y, halo_center_z]
        renderView1.CameraViewUp = [0.0, 0.0, 1.0]
        renderView1.CameraParallelScale = 10
        # Properties modified on renderView1
        renderView1.UseColorPaletteForBackground = 0

        # Properties modified on renderView1
        renderView1.Background = [0.0, 0.0, 0.0]
        
        renderView1.Update()

        azimuth_angles = [None] * num_images
        elevation_angles = [None] * num_images
        focals = [None] * num_images
        poses = [None] * num_images
        # for angle_phi in phi:
        #     for a, angle_theta in enumerate(theta):
        for a in range(num_images):
            print(a)
            # camera.Azimuth(10)      # Rotate horizontally by angle_increment
            # camera.Elevation(20 * a)  
            azimuth_angle = (a * azimuth_step) % 360  # Wrap around after 360 degrees
            elevation_angle = (a * elevation_step) % 180  # Wrap around after 90 degrees

            camera_position = spherical_to_cartesian(camera_distance, azimuth_angle, elevation_angle)
            view.CameraPosition = [camera_position[0] + renderView1.CameraFocalPoint[0],
                                camera_position[1] + renderView1.CameraFocalPoint[1],
                                camera_position[2] + renderView1.CameraFocalPoint[2]]
            view.CameraFocalPoint = renderView1.CameraFocalPoint
            view.CameraViewUp = [0, 0, 1]  # Adjust if necessary
            Render()
            # save screenshot
            # renderView1.ResetCamera() 
            # renderView1.CameraPosition = [origin[0] + r*cos(radians(angle_phi)) * cos(radians(angle_theta)), \
            #                               origin[1] + r * sin(radians(angle_phi)) * cos(radians(angle_theta)), \
            #                               origin[2] ] 
            image_filename =  "azimuth_" + str(round(azimuth_angle, 2)) + "_elevation_" + str(round(elevation_angle,2)) + '.png'
            ## compute focal length 
            view_angle = camera.GetViewAngle()
            focal = compute_focal_length(view_angle, width)
            matrix = look_at(view.CameraPosition, view.CameraFocalPoint, view.CameraViewUp)
            # Get the camera parameters
            
            camera_position = np.array(camera.GetPosition())
            focal_point = np.array(camera.GetFocalPoint())
            view_up = np.array(camera.GetViewUp())

            # Compute the camera axes
            z_axis = (camera_position - focal_point)  # Camera's backward direction
            z_axis /= np.linalg.norm(z_axis)  # Normalize to unit vector

            x_axis = np.cross(view_up, z_axis)  # Camera's right direction
            x_axis /= np.linalg.norm(x_axis)  # Normalize to unit vector

            y_axis = np.cross(z_axis, x_axis)  # Camera's up direction (orthogonal)

            # Construct the rotation matrix (camera axes as rows)
            rotation_matrix = np.array([x_axis, y_axis, z_axis])

            # Construct the translation vector (camera position)
            translation_vector = camera_position

            # Combine into a 4x4 camera-to-world transformation matrix
            camera_to_world_matrix = np.eye(4)
            camera_to_world_matrix[:3, :3] = rotation_matrix.T  # Transpose for world-to-camera to camera-to-world
            camera_to_world_matrix[:3, 3] = translation_vector
            # temp = {'azimuth': azimuth_angle, 'elevation': elevation_angle, 'focal': focal, 'pose': matrix}
            # results[a] = temp
            azimuth_angles[a] = azimuth_angle
            elevation_angles[a] = elevation_angle
            focals[a] = focal
            poses[a] = camera_to_world_matrix
            print(matrix, camera_to_world_matrix)
            # Get the near and far clipping planes
            near_plane = camera.GetClippingRange()[0]
            far_plane = camera.GetClippingRange()[1]

            # Print the near and far clipping planes
            print("Camera Near Clipping Plane:", near_plane)
            print("Camera Far Clipping Plane:", far_plane)
            # print(focal, matrix)
            SaveScreenshot(os.path.join(output_path, image_filename), renderView1, ImageResolution=[width, height])
        
        results = {'azimuth': azimuth_angles, 'elevation': elevation_angles, 'focal': focals[0], 'poses': poses}
        np.savez("/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM_Train/STEP_624_ONE_HALO/camera_params.npz", **results)
        Delete(halo_2vtu)
        del halo_2vtu
    
        # destroy convertToPointCloud1
        Delete(galaxy_2vtu)
        del galaxy_2vtu

end_time = time.perf_counter()    
print("Total time", end_time - start_time)
