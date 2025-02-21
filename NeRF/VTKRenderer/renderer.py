import pyvista as pv 
import vtk 
import numpy as np 
import os 
import matplotlib.pyplot as plt

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

halo_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/VKIN_6267_EPS_0.947/624/halo_381843336_rank_0.vtu"
halo_mesh = pv.read(halo_path) 
halo_points = halo_mesh.points
halo_vx = halo_mesh['vx']
halo_vy = halo_mesh['vy']
halo_vz = halo_mesh['vz']
v_mag = np.sqrt((halo_vx * halo_vx) + (halo_vy * halo_vy) + (halo_vz * halo_vz))

galaxy_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/VKIN_6267_EPS_0.947/624/galaxy_tag_381843336_rank_0.vtu"
galaxy_mesh = pv.read(galaxy_path)
galaxy_points = galaxy_mesh.points
print(galaxy_points.shape)


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
    
    # Create a vertex glyph filter to visualize the points
    glyph_filter = vtk.vtkVertexGlyphFilter()
    glyph_filter.SetInputData(poly_data)
    glyph_filter.Update()

    # Create a mapper and actor for the point cloud
    mapper = vtk.vtkPolyDataMapper()
    mapper.SetInputData(poly_data)
    mapper.SetInputConnection(glyph_filter.GetOutputPort())
    mapper.SetLookupTable(lookup_table)
    mapper.SetScalarRange(min(attribute), max(attribute))
    
    actor = vtk.vtkActor()
    actor.SetMapper(mapper)
    actor.GetProperty().SetPointSize(pointsize)  # Set point size
    
    return actor

halo_actor = create_point_cloud(halo_points, v_mag, 'inferno', 0.15, 1.5)
galaxy_actor = create_point_cloud(galaxy_points, galaxy_mesh['mass'], 'hot', 1.0, 4)

# Create a renderer and render window
renderer = vtk.vtkRenderer()
render_window = vtk.vtkRenderWindow()
render_window.AddRenderer(renderer)

# Create a render window interactor
render_window_interactor = vtk.vtkRenderWindowInteractor()
render_window_interactor.SetRenderWindow(render_window)

# Add the actor to the renderer
renderer.AddActor(halo_actor)
renderer.AddActor(galaxy_actor)
renderer.SetBackground(0.0, 0, 0)  # Set background color

# Start the rendering process
render_window.Render()
render_window_interactor.Start()
