import numpy as np
import cv2  # Or any library to load/save images

# Parameters
z_near = 0.1  # Near clipping plane (in meters)
z_far = 10.0  # Far clipping plane (in meters)

# Load the VTK depth map (assume it's saved as a .png or .npy file)
vtk_depth = cv2.imread('./outputs/halo_vtk/train/depth_000.png', cv2.IMREAD_UNCHANGED).astype(np.float32)
print(vtk_depth.shape)
print(np.min(vtk_depth), np.max(vtk_depth))

# Normalize depth to real-world units (for perspective projection)
z_world = (z_near * z_far) / (z_far - (vtk_depth * (z_far - z_near)))

# Optional: Rescale or clip values if needed
z_world_normalized = np.clip(z_world, z_near, z_far)

# print(z_world_normalized / z_far * 255)
# # Save normalized depth map
cv2.imwrite('depth_real_world.png', (z_world_normalized / z_far * 255).astype(np.uint8))

