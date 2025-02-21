import vtk
import math 
import numpy as np 


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
    points = generate_fibonacci_sphere(num, radius)
    for i in range(num):
        # point = random_point_on_sphere(radius)
        point = points[i]
        camera_pos = [point[0] + center[0],  point[1] + center[1], point[2] + center[2]]
        cameras[i] = camera_pos 
    return cameras   
    # points = generate_fibonacci_sphere(num, radius)
    # for p, point in enumerate(points):
    #     camera_pos = [point[0] + center[0],  point[1] + center[1], point[2] + center[2]]
    #     cameras[p] = camera_pos 
    # return cameras
  
  
def get_camera_intrinsics(width_res_in_px, height_res_in_px, view_angle_y):
    """
    Get camera intrinsics in VTK.
    
    Parameters:
        renderer (vtkRenderer): The renderer containing the scene.
        camera (vtkCamera): The VTK camera object.
    
    Returns:
        dict: A dictionary containing camera intrinsic parameters.
    """
    # Get the renderer's viewport size in pixels
    # Get the renderer's viewport size in pixels
    # render_window = renderer.GetRenderWindow()
    # width_res_in_px, height_res_in_px = render_window.GetSize()

    # Get the view angle (vertical field of view in degrees)
    # view_angle_y = camera.GetViewAngle()  # Vertical field of view (FOV)

    # Aspect ratio (width/height)
    aspect_ratio = width_res_in_px / height_res_in_px

    # Horizontal field of view
    view_angle_x = 2 * math.degrees(math.atan(math.tan(math.radians(view_angle_y) / 2) * aspect_ratio))

    # Compute focal lengths in pixel units
    focal_length_y = (height_res_in_px / 2) / math.tan(math.radians(view_angle_y) / 2)
    focal_length_x = (width_res_in_px / 2) / math.tan(math.radians(view_angle_x) / 2)

    # Optical center
    optical_center_x = width_res_in_px / 2
    optical_center_y = height_res_in_px / 2

    camera_intr_dict = {
        'camera_angle_x': view_angle_x,
        'camera_angle_y': view_angle_y,
        'fl_x': focal_length_x,
        'fl_y': focal_length_y,
        'k1': 0.0,  # Distortion coefficients are assumed to be 0 unless otherwise calibrated
        'k2': 0.0,
        'p1': 0.0,
        'p2': 0.0,
        'cx': optical_center_x,
        'cy': optical_center_y,
        'w': width_res_in_px,
        'h': height_res_in_px,
        'aabb_scale': 1.0  # No direct equivalent in VTK, set default to 1.0
    }
    
    return camera_intr_dict