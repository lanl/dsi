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

def compute_focal_length(fov_degrees, image_size):
    # Convert FOV to radians
    fov_radians = np.radians(fov_degrees)
    
    # Compute focal length
    focal_length = image_size / (2 * np.tan(fov_radians / 2))
    return focal_length

if __name__ == "__main__":
    # Example Usage
    camera_position = [0, 0, 5]
    target = [0, 0, 0]
    up = [0, 1, 0]

    matrix = look_at(camera_position, target, up)
    print(matrix)