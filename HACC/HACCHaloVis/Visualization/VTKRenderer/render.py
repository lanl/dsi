import os 
# import pyvista as pv 
import vtk 
from vtk.util import numpy_support
import numpy as np
import sys 
sys.path.append('/Users/mhan/Desktop/dsi/HACC/HACCHaloVis/utils')
from helpers import *


# Set camera position using theta, phi, and radius
def set_camera_position_spherical(camera, theta, phi, radius, center):
    x = center[0] + radius * np.sin(np.radians(phi)) * np.cos(np.radians(theta))
    y = center[1] + radius * np.sin(np.radians(phi)) * np.sin(np.radians(theta))
    z = center[2] + radius * np.cos(np.radians(phi))
    camera.SetPosition(x, y, z)
    camera.SetFocalPoint(center[0], center[1], center[2])  # Assuming the object is centered at the origin
    camera.SetViewUp(0, 0, 1)  # Keep the Z-axis as up

def render_halos(data_path, img_width, img_height, theta, phi, center, cur_output_path):
    # Create a reader
    reader = vtk.vtkXMLUnstructuredGridReader()
    reader.SetFileName(data_path)
    reader.Update()
    # Get the output data
    data = reader.GetOutput()
    bounds = data.GetBounds()
    # Extract the points
    points = data.GetPoints()
    point_data = data.GetPointData()
    vx = point_data.GetArray("vx") 
    vx_np = numpy_support.vtk_to_numpy(vx)
    vy = point_data.GetArray("vy") 
    vy_np = numpy_support.vtk_to_numpy(vy)
    vz = point_data.GetArray("vz") 
    vz_np = numpy_support.vtk_to_numpy(vz)
    # attribute_array = np.sqrt(vx_np**2 + vy_np**2 + vz_np**2)
    # Convert points to a NumPy array
    points = numpy_support.vtk_to_numpy(points.GetData())
    v_mag = np.sqrt(vx_np**2 + vy_np**2 + vz_np**2)
    # print(bounds)
    # data = pv.read(data_path)
    # points = data.points
    # v_mag = data['v_mag']
    # bounds = data.bounds 
    ## normalize data to [-1, 1] and center is [0, 0, 0]
    # points[:, 0] = 2 * ((points[:, 0] - bounds[0]) / (bounds[1] - bounds[0])) - 1
    # points[:, 1] = 2 * ((points[:, 1] - bounds[2]) / (bounds[3] - bounds[2])) - 1
    # points[:, 2] = 2 * ((points[:, 2] - bounds[4]) / (bounds[5] - bounds[4])) - 1
    # print(bounds)
    halo_actor = create_point_cloud(points, v_mag, 'inferno', 1.0, 8)

    # Create a renderer
    renderer = vtk.vtkRenderer()
    renderer.AddActor(halo_actor)
    renderer.ResetCamera()
    renderer.SetBackground(0.0, 0.0, 0.0)  # Set background color

    # Create a render window
    render_window = vtk.vtkRenderWindow()
    render_window.AddRenderer(renderer)
    render_window.SetSize(img_width, img_height)
    render_window.SetMultiSamples(8)
    # # Camera setup
    # camera = renderer.GetActiveCamera()
    # # print(center)
    # # print(camera.GetPosition())
    # camera.SetPosition(center[0] + 8, center[1] + 8, center[2] + 8)
    # camera.SetFocalPoint(center[0], center[1], center[2]) 
    # camera.SetViewUp(0, 0, 1)
    # for angle_phi in phi:
    #     for a, angle_theta in enumerate(theta):
    #         # set_camera_position_spherical(camera, angle_theta, angle_phi, 5, center)
    #         # print(camera.GetPosition())
    #         # Set azimuth and elevation
    #         camera.Azimuth(90)  # Rotate 30 degrees around the view up vector
    #         camera.Elevation(45)  # Rotate 20 degrees around the horizontal axis

    #         # Ensure the view up vector is orthogonal to the view plane normal
    #         camera.OrthogonalizeViewUp()
    # #         # Capture image
    #         render_window.Render()
    #         window_to_image_filter = vtk.vtkWindowToImageFilter()
    #         window_to_image_filter.SetInputBufferTypeToRGBA()
    #         window_to_image_filter.SetInput(render_window)
    #         window_to_image_filter.SetScale(1)  # No scaling
    #         window_to_image_filter.Update()
    #         writer = vtk.vtkPNGWriter()
    #         image_name = "phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
    #         image_name = os.path.join(cur_output_path, image_name)
    #         # image_name = "./test.png"
    #         writer.SetFileName(image_name)
    #         writer.SetInputConnection(window_to_image_filter.GetOutputPort())
    #         writer.Write()
    # render_window.SetOffScreenRendering(True)

    # Create an interactor
    # interactor = vtk.vtkRenderWindowInteractor()
    # interactor.SetRenderWindow(render_window)
    
    # interactor.GetRenderWindow().SetDisplayId("_0_p_void")

    # # Render and start interaction
    # render_window.Render()
    # interactor.Start()

    return renderer, render_window
    
    