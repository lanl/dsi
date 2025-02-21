# trace generated using paraview version 5.13.0
#import paraview
#paraview.compatibility.major = 5
#paraview.compatibility.minor = 13

#### import the simple module from the paraview
from paraview.simple import *
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()

import os
import math 
import sys 
sys.path.append("/Users/mhan/Desktop/Examples/VTKRenderer/")
from compute_pose_focal import *
import numpy as np

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

def generate_transform_matrix(pos, rot, focus):

    def Rx(theta):
      return np.matrix([[ 1, 0            , 0            ],
                        [ 0, np.cos(theta),-np.sin(theta)],
                        [ 0, np.sin(theta), np.cos(theta)]])
    def Ry(theta):
      return np.matrix([[ np.cos(theta), 0, np.sin(theta)],
                        [ 0            , 1, 0            ],
                        [-np.sin(theta), 0, np.cos(theta)]])
    def Rz(theta):
      return np.matrix([[ np.cos(theta), -np.sin(theta), 0 ],
                        [ np.sin(theta), np.cos(theta) , 0 ],
                        [ 0            , 0             , 1 ]])

    R = Rz(-math.radians(rot[2])) * Ry(-math.radians(rot[1])) * Rx(-math.radians(rot[0]))
    xf_rot = np.eye(4)
    xf_rot[:3,:3] = R

    xf_pos = np.eye(4)
    xf_pos[:3,3] = np.array(pos) - np.array([focus[0], focus[1], focus[2]])

    extra_xf = np.matrix([
        [-1, 0, 0, 0],
        [ 0, 0, 1, 0],
        [ 0, 1, 0, 0],
        [ 0, 0, 0, 1]])
    
    shift_coords = np.matrix([
        [0, 0, 1, 0],
        [1, 0, 0, 0],
        [0, 1, 0, 0],
        [0, 0, 0, 1]])
    xf = shift_coords @ extra_xf @ xf_pos
    assert np.abs(np.linalg.det(xf) - 1.0) < 1e-4
    xf = xf @ xf_rot
    return np.float32(xf).tolist()

output_path = "/Users/mhan/Desktop/data/PV_InSitu/PV_INSITU_TRAIN_ONETS/images"
# create a new 'XML Image Data Reader'
pv_insitu_300x300x300_32662vti = XMLImageDataReader(registrationName='pv_insitu_300x300x300_32662.vti', FileName=['/Users/mhan/Desktop/data/PV_InSitu/pv_insitu_300x300x300_32662.vti'])

# Properties modified on pv_insitu_300x300x300_32662vti
pv_insitu_300x300x300_32662vti.TimeArray = 'None'

# get active view
renderView1 = GetActiveViewOrCreate('RenderView')

# show data in view
pv_insitu_300x300x300_32662vtiDisplay = Show(pv_insitu_300x300x300_32662vti, renderView1, 'UniformGridRepresentation')

# trace defaults for the display properties.
pv_insitu_300x300x300_32662vtiDisplay.Representation = 'Outline'

# reset view to fit data
renderView1.ResetCamera(False, 0.9)

# get the material library
materialLibrary1 = GetMaterialLibrary()

renderView1.OrientationAxesVisibility = 0

# update the view to ensure updated data information
renderView1.Update()

# set scalar coloring
ColorBy(pv_insitu_300x300x300_32662vtiDisplay, ('POINTS', 'grd'))

# rescale color and/or opacity maps used to include current data range
pv_insitu_300x300x300_32662vtiDisplay.RescaleTransferFunctionToDataRange(True, True)

# change representation type
pv_insitu_300x300x300_32662vtiDisplay.SetRepresentationType('Volume')

# rescale color and/or opacity maps used to include current data range
pv_insitu_300x300x300_32662vtiDisplay.RescaleTransferFunctionToDataRange(True, False)

# get color transfer function/color map for 'grd'
grdLUT = GetColorTransferFunction('grd')

# get opacity transfer function/opacity map for 'grd'
grdPWF = GetOpacityTransferFunction('grd')

# get 2D transfer function for 'grd'
grdTF2D = GetTransferFunction2D('grd')

# set scalar coloring
ColorBy(pv_insitu_300x300x300_32662vtiDisplay, ('POINTS', 'v02'))

# Hide the scalar bar for this color map if no visible data is colored by it.
HideScalarBarIfNotNeeded(grdLUT, renderView1)

# rescale color and/or opacity maps used to include current data range
pv_insitu_300x300x300_32662vtiDisplay.RescaleTransferFunctionToDataRange(True, False)

# show color bar/color legend
pv_insitu_300x300x300_32662vtiDisplay.SetScalarBarVisibility(renderView1, True)

# get color transfer function/color map for 'v02'
v02LUT = GetColorTransferFunction('v02')

# get opacity transfer function/opacity map for 'v02'
v02PWF = GetOpacityTransferFunction('v02')

# get 2D transfer function for 'v02'
v02TF2D = GetTransferFunction2D('v02')

# get color legend/bar for v02LUT in view renderView1
v02LUTColorBar = GetScalarBar(v02LUT, renderView1)

# hide color bar/color legend
pv_insitu_300x300x300_32662vtiDisplay.SetScalarBarVisibility(renderView1, False)

#================================================================
# addendum: following script captures some of the application
# state to faithfully reproduce the visualization during playback
#================================================================

# get layout
layout1 = GetLayout()

#--------------------------------
# saving layout sizes for layouts

# layout/tab size in pixels
layout1.SetSize(width, height)

view = GetActiveView()
camera = GetActiveCamera()
#-----------------------------------
# saving camera placements for views

# current camera placement for renderView1
renderView1.CameraPosition = [-2326561.674287138, 2861151.818533813, 10975749.650547516]
renderView1.CameraFocalPoint = [0.0, 900000.0, 0.0]
renderView1.CameraViewUp = [0.06311849447957099, 0.9846772446244371, -0.16256315564560997]
renderView1.CameraParallelScale = 2947880.594596735
camera_distance = math.dist(renderView1.CameraPosition, renderView1.CameraFocalPoint)
azimuth_angles = [None] * num_images
elevation_angles = [None] * num_images
focals = [None] * num_images
poses = [None] * num_images
near_planes = [None] * num_images
far_planes = [None] * num_images

for a in range(num_images):
    print(a)
    # camera.Azimuth(10)      # Rotate horizontally by angle_increment
    # camera.Elevation(20 * a)  
    azimuth_angle = (a * azimuth_step) % 360  # Wrap around after 360 degrees
    elevation_angle = (a * elevation_step) % 180  # Wrap around after 90 degrees
    azimuth_angles[a] = azimuth_angle
    elevation_angles[a] = elevation_angle
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
    # matrix = look_at(view.CameraPosition, view.CameraFocalPoint, view.CameraViewUp)
    
    focals[a] = focal
    # Get the camera parameters
    
    # camera_position = np.array(camera.GetPosition())
    # focal_point = np.array(camera.GetFocalPoint())
    # view_up = np.array(camera.GetViewUp())
    

    # # Compute the camera axes
    # backward = (camera_position - focal_point)  # Camera's backward direction
    # backward /= np.linalg.norm(backward)  # Normalize to unit vector

    # right = np.cross(view_up, backward)  # Camera's right direction
    # right /= np.linalg.norm(right)  # Normalize to unit vector

    # up = np.cross(backward, right)  # Camera's true up direction (orthogonal)

    # # Construct the rotation matrix (camera axes as rows)
    # rotation_matrix = np.array([right, up, backward])

    # # Construct the translation vector (camera position)
    # translation_vector = camera_position

    # # Combine into a 4x4 camera-to-world transformation matrix
    # camera_to_world_matrix = np.eye(4)
    # camera_to_world_matrix[:3, :3] = rotation_matrix.T  # Transpose to match the world-to-camera convention
    # camera_to_world_matrix[:3, 3] = translation_vector
    
    camera_to_world_matrix = generate_transform_matrix(camera.GetPosition(), camera.GetOrientation(), view.CameraFocalPoint)
    poses[a] = camera_to_world_matrix
    # print(matrix, camera_to_world_matrix)
    # Get the near and far clipping planes
    near_plane = camera.GetClippingRange()[0]
    far_plane = camera.GetClippingRange()[1]
    near_planes[a] = near_plane
    far_planes[a] = far_plane

    # Print the near and far clipping planes
    # print("Camera Near Clipping Plane:", near_plane)
    # print("Camera Far Clipping Plane:", far_plane)
    # print(focal, matrix)
    SaveScreenshot(os.path.join(output_path, image_filename), renderView1, ImageResolution=[width, height])

results = {'azimuth': azimuth_angles, 'elevation': elevation_angles, 'focal': focals[0], 'poses': poses, 'near': near_planes, 'far': far_planes}
# np.savez("/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM_Train/STEP_624_ONE_HALO/camera_params.npz", **results)
np.savez("/Users/mhan/Desktop/data/PV_InSitu/PV_INSITU_TRAIN_ONETS/camera_params.npz", **results)

# destroy pv_insitu_300x300x300_32662vti
Delete(pv_insitu_300x300x300_32662vti)
del pv_insitu_300x300x300_32662vti

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