import os 
from cdb import *
import vtk
import matplotlib.pyplot as plt 

def match_time_steps(run_directory):
    # haloproperties_ts = []
    #     full_res_ts = []
    with open(os.path.join(run_directory, 'params', 'indat.params'), 'r') as file:
        for line in file:
            splits = line.split(" ")
            if(splits[0] == 'PK_DUMP'):
                haloproperties_ts = sorted([int(t) for t in splits[1:]])
            elif(splits[0] == 'FULL_ALIVE_DUMP'):
                full_res_ts = sorted([int(t) for t in splits[1:]])
    
    return haloproperties_ts, full_res_ts

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    #     return 0
    # else: return 1
    
def get_angles(phi_angle, theta_angle):
    phi = []
    for angle in range(-180, 181, phi_angle):
        phi.append( float(angle) )

    theta = []
    for angle in range(-90, 91, theta_angle):
        theta.append( float(angle) )
    return [phi, theta]

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
    actor.GetProperty().SetRenderPointsAsSpheres(True)
    actor.GetProperty().SetPointSize(pointsize)  # Set point size
    
    return actor
    
# def addEntryToCDB(cdbDatabase, 