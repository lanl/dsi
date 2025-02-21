import vtk 
import numpy as np 
import os 
import matplotlib.cm as cm
from vtk_camera import get_camera_intrinsics, gen_cameras
from vtk.util import numpy_support
from PIL import Image
import json 
import random 

width = 512
height = 512


if __name__ == "__main__":
    output_path = "./outputs/scalarflow_50x50/val/"
    data_path = "./data/ScalarFlow/"
    filenames = sorted([f for f in os.listdir(data_path) if f.startswith('velocity')])[100:150]
    # num_ts = len(filenames)
    num_ts = 10
    indices = list(range(50))
    print("num_ts", num_ts)
    random_elements = sorted(random.sample(indices, num_ts))
    num_img_per_img = 50
    num_imgs = num_ts * num_img_per_img
    
    frames = [None] * num_imgs
    output_data = None
    center = [0.0, 0, 0.0]
    radius = 5
    camera_pos_list = gen_cameras(num_img_per_img, center, radius)
    time_interval = 1.0 / len(filenames)
    
    # for f, filename in enumerate(filenames):
    for f, index in enumerate(random_elements):
        filename = filenames[index]
        # Read the VTI file
        reader = vtk.vtkXMLImageDataReader()
        reader.SetFileName(os.path.join(data_path, filename))
        reader.Update()
        # Extract the scalar data (velocity magnitude)
        scalar_name = "Scalars_"  # Ensure this matches the exact name in your file
        image_data = reader.GetOutput()

        # Get the scalar range
        # scalar_range = image_data.GetPointData().GetScalars(scalar_name).GetRange()
        # print(scalar_range)
        # scalar_range = [0, 6.7744]
        # scalar_range = [0, 4.5]

        # # Create a volume property
        # volume_property = vtk.vtkVolumeProperty()
        # volume_property.ShadeOn()
        # volume_property.SetInterpolationTypeToLinear()
        # Apply the contour filter (isosurface extraction)
        contour_filter = vtk.vtkContourFilter()
        contour_filter.SetInputData(image_data)
        contour_filter.SetValue(0, 0.9)  # Isosurface at value 0.9
        contour_filter.Update()
        
        # Create Turbo colormap from Matplotlib
        # turbo_colormap = cm.get_cmap("turbo", 256)  # Get Turbo colormap with 256 points

        # Create VTK Color Transfer Function
        # color_transfer_function = vtk.vtkColorTransferFunction()
        # for i in range(256):
        #     value = scalar_range[0] + (i / 255.0) * (scalar_range[1] - scalar_range[0])  # Map to scalar range
        #     r, g, b, _ = turbo_colormap(i / 255.0)  # Get Turbo colormap color
        #     color_transfer_function.AddRGBPoint(value, r, g, b)

        # # Transfer function (maps scalar values to colors)
        # color_transfer_function = vtk.vtkColorTransferFunction()
        # color_transfer_function.AddRGBPoint(scalar_range[0], 0.0, 0.0, 1.0)  # Blue for min values
        # color_transfer_function.AddRGBPoint((scalar_range[0] + scalar_range[1]) / 2, 0.0, 1.0, 0.0)  # Green for mid values
        # color_transfer_function.AddRGBPoint(scalar_range[1], 1.0, 0.0, 0.0)  # Red for max values

        # Opacity transfer function (controls transparency)
        # opacity_transfer_function = vtk.vtkPiecewiseFunction()
        # opacity_transfer_function.AddPoint(scalar_range[0], 0.0)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(0.785, 0.1)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(0.936, 0.192)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(1.339, 0.585)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(1.178, 1.0)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint((scalar_range[0] + scalar_range[1]) / 2, 0.5)  # Semi-transparent mid value
        # opacity_transfer_function.AddPoint(1.037, 0.0)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(1.280, 0.192)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(1.783, 0.567)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(2.350, 1.0)  # Completely transparent at min value
        # opacity_transfer_function.AddPoint(scalar_range[1], 1.0)  # Fully opaque at max value

        # Apply transfer functions to volume property
        # volume_property.SetColor(color_transfer_function)
        # volume_property.SetScalarOpacity(opacity_transfer_function)

        # Create volume mapper
        # volume_mapper = vtk.vtkSmartVolumeMapper()
        # volume_mapper.SetInputData(image_data)

        # Create volume actor
        # volume = vtk.vtkVolume()
        # volume.SetMapper(volume_mapper)
        # volume.SetProperty(volume_property)
        
        # Create a mapper for the isosurface
        mapper = vtk.vtkPolyDataMapper()
        mapper.SetInputConnection(contour_filter.GetOutputPort())
        mapper.ScalarVisibilityOff()  # Use a solid color instead of scalar colors
        # Create an actor for the isosurface
        actor = vtk.vtkActor()
        actor.SetMapper(mapper)

        # Set actor color
        # actor.GetProperty().SetColor(1.0, 0.0, 0.0)  # Red color
        actor.GetProperty().SetColor(34.0 / 255.0, 91.0/255.0, 151.0 / 255.0)
        # actor.GetProperty().SetOpacity(0.95)  # Slight transparency
        
        # Renderer setup
        renderer = vtk.vtkRenderer()
        # renderer.AddVolume(volume)
        renderer.AddActor(actor)
        renderer.SetBackground(1.0, 1.0, 1.0)  # Dark background

        # Render window
        render_window = vtk.vtkRenderWindow()
        render_window.AddRenderer(renderer)
        render_window.SetSize(width, height)
        render_window.SetMultiSamples(8)
        render_window.SetOffScreenRendering(True)
        
        # Interactor for user interaction
        # interactor = vtk.vtkRenderWindowInteractor()
        # interactor.SetRenderWindow(render_window)
        
        
        # Start rendering
        # render_window.Render()
        # interactor.Start()
        
        camera = renderer.GetActiveCamera()
        view_angle_y = camera.GetViewAngle()
        
        if f == 0:
            output_data = get_camera_intrinsics(width, height, view_angle_y)
        
        for c, camera_pos in enumerate(camera_pos_list):
            camera_pos = camera_pos_list[c]
            # print(f, camera_pos)
            renderer.ResetCamera()
            camera.SetPosition(camera_pos[0], camera_pos[1], camera_pos[2])
            camera.SetFocalPoint(center[0], center[1], center[2])
            camera.SetViewUp(0, 1, 0)
            renderer.ResetCameraClippingRange()
        
            [near, far] = camera.GetClippingRange()
            # print(near, far)
            camera.SetClippingRange(3, 11)
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
            image_name = "img_" + str(f * num_img_per_img + c).zfill(3)
            image_writer.SetFileName(os.path.join(output_path, image_name + '.png'))
            image_writer.SetInputConnection(window_to_image_filter.GetOutputPort())
            image_writer.Write()
            temp_frame = {'file_path': "train/" + image_name, "time": f * time_interval, 'transform_matrix': c2w.tolist()} #'depth_file_path': "./train/" + depth_name,
            frames[f * num_img_per_img + c] = temp_frame
    
    output_data['frames'] = frames
    with open(os.path.join(output_path, "transforms.json") , "w") as outfile:
        json.dump(output_data, outfile, indent=4)       
        # depth_name = "depth_" + str(f).zfill(3) + '.png'
        # window_to_depth_filter = vtk.vtkWindowToImageFilter()
        # window_to_depth_filter.SetInput(render_window)
        # window_to_depth_filter.SetInputBufferTypeToZBuffer()
        # window_to_depth_filter.Update()
        # # Convert the depth buffer to a NumPy array
        # depth_image = window_to_depth_filter.GetOutput()
        # width, height, _ = depth_image.GetDimensions()
        # depth_array = numpy_support.vtk_to_numpy(depth_image.GetPointData().GetScalars())
        # depth_array = depth_array.reshape(width, height)
        # depth_array = np.flip(depth_array, axis=0)

        # # Normalize and save the depth map
        # depth_array = (depth_array - depth_array.min()) / (depth_array.max() - depth_array.min())  # Normalize
        # depth_map_image = (depth_array * 255).astype(np.uint8)  # Convert to 8-bit image
        # depth_map = Image.fromarray(depth_map_image)
        # depth_map.save(os.path.join(output_path, "depth_" + str(f).zfill(3) + '.png'))
