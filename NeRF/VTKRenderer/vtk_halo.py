import pyvista as pv 
import vtk 
import numpy as np 
import os 
import matplotlib.pyplot as plt
from vtk_camera import get_camera_intrinsics, gen_cameras
import json 
from PIL import Image
from vtk.util import numpy_support

width = 1024
height = 1024
radius = 5
num_imgs = 50
output_path = "./outputs/halo_vtk_50/"
halo_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/VKIN_6267_EPS_0.947/624/halo_0.vtu"
halo_mesh = pv.read(halo_path) 
halo_points = halo_mesh.points
v_mag = halo_mesh['v_mag']
# bounds = halo_mesh['bounds']
bounds = halo_mesh.bounds
# center = [(bounds[1] - bounds[0]) / 2.0 + bounds[0], (bounds[3] - bounds[2]) / 2.0 + bounds[2], (bounds[5] - bounds[4]) / 2.0 + bounds[4]]
halo_points[:, 0] = 2 * (halo_points[:, 0] - bounds[0]) / (bounds[1] - bounds[0]) - 1
halo_points[:, 1] = 2 * (halo_points[:, 1] - bounds[2]) / (bounds[3] - bounds[2]) - 1
halo_points[:, 2] = 2 * (halo_points[:, 2] - bounds[4]) / (bounds[5] - bounds[4]) - 1
center = [0.0, 0.0, 0.0]
camera_pos_list = gen_cameras(num_imgs, center, radius)
# halo_vx = halo_mesh['vx']
# halo_vy = halo_mesh['vy']
# halo_vz = halo_mesh['vz']
# v_mag = np.sqrt((halo_vx * halo_vx) + (halo_vy * halo_vy) + (halo_vz * halo_vz))

# galaxy_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/VKIN_6267_EPS_0.947/624/galaxy_tag_381843336_rank_0.vtu"
# galaxy_mesh = pv.read(galaxy_path)
# galaxy_points = galaxy_mesh.points
# print(galaxy_points.shape)


# Function to create a vtkPolyData object from a point cloud
def create_point_cloud(points, attribute, colormap_name="inferno", opacity=0.5, pointsize=1):
    
    # Create vtkPoints and vtkFloatArray for scalars
    vtk_points = vtk.vtkPoints()
    scalar_array = vtk.vtkFloatArray()
    scalar_array.SetName("scalar")
    
    for point, scalar in zip(points, attribute):
        vtk_points.InsertNextPoint(point)
        scalar_array.InsertNextValue(scalar)
    
    # Create vtkPolyData and add points and scalars
    poly_data = vtk.vtkPolyData()
    poly_data.SetPoints(vtk_points)
    poly_data.GetPointData().SetScalars(scalar_array)
    
    # Create a vtkLookupTable using a Matplotlib colormap
    colormap = plt.get_cmap(colormap_name)
    lookup_table = vtk.vtkLookupTable()
    lookup_table.SetNumberOfTableValues(256)
    lookup_table.Build()
    
    for i in range(256):
        rgba = colormap(i / 255.0)
        lookup_table.SetTableValue(i, rgba[0], rgba[1], rgba[2], opacity)
    
    # Create a sphere source to use as glyph
    # sphere_source = vtk.vtkSphereSource()
    # sphere_source.SetRadius(pointsize)  # Adjust the radius as needed
    # sphere_source.SetPhiResolution(10)  # Increase for smoother spheres
    # sphere_source.SetThetaResolution(10)
    
    # Create a vertex glyph filter to visualize the points
    glyph_filter = vtk.vtkVertexGlyphFilter()
    glyph_filter.SetInputData(poly_data)
    glyph_filter.Update()
    # Create a glyph filter to apply spheres at each point
    # glyph_filter = vtk.vtkGlyph3D()
    # glyph_filter.SetSourceConnection(sphere_source.GetOutputPort())
    # glyph_filter.SetInputData(poly_data)
    # glyph_filter.Update()

    # Create a mapper and actor for the point cloud
    mapper = vtk.vtkPolyDataMapper()
    # mapper.SetInputData(poly_data)
    mapper.SetInputConnection(glyph_filter.GetOutputPort())
    mapper.SetLookupTable(lookup_table)
    mapper.SetScalarRange(min(attribute), max(attribute))
    
    actor = vtk.vtkActor()
    actor.SetMapper(mapper)
    actor.GetProperty().SetRenderPointsAsSpheres(1)  # Render as circular points
    actor.GetProperty().SetPointSize(pointsize) 
    
    return actor

halo_actor = create_point_cloud(halo_points, v_mag, 'inferno', 1.0, 5)
# galaxy_actor = create_point_cloud(galaxy_points, galaxy_mesh['mass'], 'hot', 1.0, 4)

# Create a renderer and render window
renderer = vtk.vtkRenderer()
render_window = vtk.vtkRenderWindow()
render_window.SetSize(width, height)
render_window.AddRenderer(renderer)
render_window.SetMultiSamples(8)
render_window.SetOffScreenRendering(True)

# Create a render window interactor
render_window_interactor = vtk.vtkRenderWindowInteractor()
render_window_interactor.SetRenderWindow(render_window)

# Add the actor to the renderer
renderer.AddActor(halo_actor)
# renderer.AddActor(galaxy_actor)
renderer.SetBackground(1, 1, 1)  # Set background color

# # Start the rendering process
# render_window.Render()
# render_window_interactor.Start()

camera = renderer.GetActiveCamera()
    
output_data = get_camera_intrinsics(renderer, camera)
frames = [None] * num_imgs

for i in range(num_imgs):
    camera_pos = camera_pos_list[i]
    renderer.ResetCamera()
    camera.SetPosition(camera_pos[0], camera_pos[1], camera_pos[2])
    camera.SetFocalPoint(center[0], center[1], center[2])
    renderer.ResetCameraClippingRange()
    
    [near, far] = camera.GetClippingRange()
    # print(near, far)
    
    # Get the world-to-camera matrix
    world_to_camera_matrix = camera.GetViewTransformMatrix()

    # Get the camera-to-world matrix (inverse of world-to-camera)
    camera_to_world_matrix = vtk.vtkMatrix4x4()
    camera_to_world_matrix.DeepCopy(world_to_camera_matrix)
    camera_to_world_matrix.Invert()
    c2w = np.zeros((4, 4))
    for m in range(4):
        for n in range(4):
            c2w[m][n] = camera_to_world_matrix.GetElement(m, n)
    # Warm-up frames
    # for n in range(1):  # Render 10 warm-up frames
    render_window.Render()
    window_to_image_filter = vtk.vtkWindowToImageFilter()
    window_to_image_filter.SetInput(render_window)
    window_to_image_filter.SetInputBufferTypeToRGBA() 
    window_to_image_filter.Update()
        # print("debug 2")
        # Use vtkPNGWriter to write the image to a file
    image_writer = vtk.vtkPNGWriter()
    image_name = "img_" + str(i).zfill(3) + '.png'
    image_writer.SetFileName(os.path.join(output_path, "img_" + str(i).zfill(3) + '.png'))
    image_writer.SetInputConnection(window_to_image_filter.GetOutputPort())
    image_writer.Write()
    
    depth_name = "depth_" + str(i).zfill(3) + '.png'
    window_to_depth_filter = vtk.vtkWindowToImageFilter()
    window_to_depth_filter.SetInput(render_window)
    window_to_depth_filter.SetInputBufferTypeToZBuffer()
    window_to_depth_filter.Update()
    # Convert the depth buffer to a NumPy array
    depth_image = window_to_depth_filter.GetOutput()
    width, height, _ = depth_image.GetDimensions()
    depth_array = numpy_support.vtk_to_numpy(depth_image.GetPointData().GetScalars())
    depth_array = depth_array.reshape(width, height)
    depth_array = np.flip(depth_array, axis=0)

    # Normalize and save the depth map
    depth_array = (depth_array - depth_array.min()) / (depth_array.max() - depth_array.min())  # Normalize
    depth_map_image = (depth_array * 255).astype(np.uint8)  # Convert to 8-bit image
    depth_map = Image.fromarray(depth_map_image)
    depth_map.save(os.path.join(output_path, "depth_" + str(i).zfill(3) + '.png'))
    # depth_writer = vtk.vtkTIFFWriter()
    # depth_writer.SetFileName(os.path.join(output_path, "depth_" + str(i).zfill(3) + '.png'))
    # depth_writer.SetInputConnection(window_to_depth_filter.GetOutputPort())
    # depth_writer.Write()
    temp_frame = {'file_path': "./" + image_name, 'depth_file_path': "./" + depth_name, 'transform_matrix': c2w.tolist()} #'depth_file_path': "./train/" + depth_name,
    frames[i] = temp_frame

output_data['frames'] = frames
with open(os.path.join(output_path, "transforms.json") , "w") as outfile:
    json.dump(output_data, outfile, indent=4)