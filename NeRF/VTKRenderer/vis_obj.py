import numpy as np 
import vtk 
import math 
import os 
from vtk.util import numpy_support
import json 
from vtk_camera import get_camera_intrinsics, gen_cameras

img_width = 1920
img_height = 1080
num_imgs = 100
radius = 4
output_path = "./outputs/duck_vtk/train"

def look_at(camera_position, target, up):
    # Step 1: Calculate Forward (F)
    F = np.array(target) - np.array(camera_position)
    F = F / np.linalg.norm(F)  # Normalize

    # Step 2: Calculate Right (R)
    up = np.array(up)
    R = np.cross(F, up)
    R = R / np.linalg.norm(R)  # Normalize

    # Step 3: Calculate Recomputed Up (U')
    U = np.cross(R, F)

    # Step 4: Construct Rotation Matrix
    rotation = np.array([
        [R[0], U[0], -F[0], 0],
        [R[1], U[1], -F[1], 0],
        [R[2], U[2], -F[2], 0],
        [0,     0,     0,    1]
    ])

    # Step 5: Construct Translation Matrix
    translation = np.array([
        [1, 0, 0, -camera_position[0]],
        [0, 1, 0, -camera_position[1]],
        [0, 0, 1, -camera_position[2]],
        [0, 0, 0, 1]
    ])

    # Step 6: Combine
    view_matrix = np.dot(rotation, translation)
    return view_matrix

def generate_fibonacci_sphere(n_points, radius):
    increment = math.pi * (3 - math.sqrt(5))
    offset = 2  / n_points;
    points = [None] * n_points
    
    for i in range(n_points):
        y = ((i * offset) - 1) + offset / 2
        r = math.sqrt(1 - y * y)
        phi = i * increment
        x = r * math.cos(phi)
        z = r * math.sin(phi)
        points[i] = [x * radius, y * radius, z * radius]
    return points

# Function to generate a random point on a sphere
def random_point_on_sphere(radius=5):
    theta = np.random.uniform(0, 2 * np.pi)  # Random angle in [0, 2π]
    phi = np.random.uniform(0, np.pi)        # Random angle in [0, π]

    # Spherical to Cartesian coordinates
    x = radius * np.sin(phi) * np.cos(theta)
    y = radius * np.sin(phi) * np.sin(theta)
    z = radius * np.cos(phi)

    return x, y, z

def gen_cameras(num, center, radius):
    cameras = [None] * num
    for i in range(num):
        point = random_point_on_sphere(radius)
        camera_pos = [point[0] + center[0],  point[1] + center[1], point[2] + center[2]]
        cameras[i] = camera_pos 
    return cameras   
    # points = generate_fibonacci_sphere(num, radius)
    # for p, point in enumerate(points):
    #     camera_pos = [point[0] + center[0],  point[1] + center[1], point[2] + center[2]]
    #     cameras[p] = camera_pos 
    # return cameras
    
if __name__ == "__main__":
    

    # Read the OBJ file
    reader = vtk.vtkOBJReader()
    reader.SetFileName("/Users/mhan/Downloads/bob/bob_tri.obj")
    reader.Update()
    # Read the texture image
    readerTexture = vtk.vtkPNGReader()
    readerTexture.SetFileName("/Users/mhan/Downloads/bob/bob_diffuse.png")

    # Create a texture object
    texture = vtk.vtkTexture()
    texture.SetInputConnection(readerTexture.GetOutputPort())
    
    # Create a mapper
    mapper = vtk.vtkPolyDataMapper()
    mapper.SetInputConnection(reader.GetOutputPort())
    
    polydata = reader.GetOutput()
    bounds = polydata.GetBounds()
    print("bounds", bounds)
    center = [(bounds[1] - bounds[0]) / 2.0 + bounds[0], (bounds[3] - bounds[2]) / 2.0 + bounds[2], (bounds[5] - bounds[4]) / 2.0 + bounds[4]]
    camera_pos_list = gen_cameras(num_imgs, center, radius)
    
    # Create an actor
    actor = vtk.vtkActor()
    actor.SetMapper(mapper)
    actor.SetTexture(texture)
    
    # Create a renderer and add the actor
    renderer = vtk.vtkRenderer()
    renderer.AddActor(actor)

    renderer.ResetCamera()  
    renderer.SetBackground(0.0, 0.0, 0.0)  # Set background color

    
    # Create a render window
    renderWindow = vtk.vtkRenderWindow()
    renderWindow.AddRenderer(renderer)
    renderWindow.SetSize(img_width, img_height)
    renderWindow.SetOffScreenRendering(True)

    # Create an interactor
    renderWindowInteractor = vtk.vtkRenderWindowInteractor()
    renderWindowInteractor.SetRenderWindow(renderWindow)
    
    camera = renderer.GetActiveCamera()
    
    output_data = get_camera_intrinsics(renderer, camera)
    
    # print(camera_intrinsics)
    frames = [None] * num_imgs
    
    for i in range(num_imgs):
        camera_pos = camera_pos_list[i]
        renderer.ResetCamera()
        camera.SetPosition(camera_pos[0], camera_pos[1], camera_pos[2])
        camera.SetFocalPoint(center[0], center[1], center[2])
        renderer.ResetCameraClippingRange()
        
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
        # if i == 0:
        #     print(camera_to_world_matrix)
        #     print(c2w)
        
        renderWindow.Render()
        window_to_image_filter = vtk.vtkWindowToImageFilter()
        window_to_image_filter.SetInput(renderWindow)
        window_to_image_filter.Update()
            # print("debug 2")
            # Use vtkPNGWriter to write the image to a file
        image_writer = vtk.vtkPNGWriter()
        image_name = os.path.join(output_path, "img_" + str(i).zfill(3) + '.png')
        image_writer.SetFileName(image_name)
        image_writer.SetInputConnection(window_to_image_filter.GetOutputPort())
        image_writer.Write()
        
        depth_name = os.path.join(output_path, "depth_" + str(i).zfill(3) + '.png')
        window_to_depth_filter = vtk.vtkWindowToImageFilter()
        window_to_depth_filter.SetInput(renderWindow)
        window_to_depth_filter.SetInputBufferTypeToZBuffer()
        window_to_depth_filter.Update()
        depth_writer = vtk.vtkTIFFWriter()
        depth_writer.SetFileName(depth_name)
        depth_writer.SetInputConnection(window_to_depth_filter.GetOutputPort())
        depth_writer.Write()
        temp_frame = {'file_path': image_name, 'depth_file_path': depth_name, 'transform_matrix': c2w.tolist()}
        frames[i] = temp_frame

    output_data['frames'] = frames
    with open(os.path.join(output_path, "transforms.json") , "w") as outfile:
        json.dump(output_data, outfile, indent=4)
    
    
    
    
    
    # Start the rendering process
    # renderWindowInteractor.Initialize()
    # renderWindow.Render()
    # renderWindowInteractor.Start()
