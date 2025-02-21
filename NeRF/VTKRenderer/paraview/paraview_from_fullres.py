import os
import time
import pandas as pd
# import sys
# if '--virtual-env' in sys.argv:
#     virtualEnvPath = sys.argv[sys.argv.index('--virtual-env') + 1]
#     print("virtualEnvPath: ", virtualEnvPath)
#     # Linux
#     virtualEnv = virtualEnvPath + '/bin/activate_this.py'
#     # Windows
#     # virtualEnv = virtualEnvPath + '/Scripts/activate_this.py'
#     if sys.version_info.major < 3:
#       execfile(virtualEnv, dict(__file__=virtualEnv))
#     else:
#       exec(open(virtualEnv).read(), {'__file__': virtualEnv})
      
from paraview.simple import *
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()
from math import cos, radians, sin

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        return 0
    else: return 1
    
def get_angles(phi_angle, theta_angle):
    phi = []
    for angle in range(-180, 181, phi_angle):
        phi.append( float(angle) )

    theta = []
    for angle in range(0, 180, theta_angle):
        theta.append( float(angle) )
    return [phi, theta]

total_start_time = time.perf_counter()

ts = 59
radius = 2
aPhi = 60
aTheta = 40
[phi, theta] = get_angles(aPhi, aTheta)

## Load Plugin 
# LoadPlugin("GenericIOReader", remote=True, ns=globals())

 ## image size 
img_width = 512
img_height = 512

n_halos = 50
halo_info_filename = "/Users/mhan/Desktop/Projects/halo_vis_paraview/data/halo_info/3A/largest_50_A_59.csv"
## Read n_halos halo_info 
csv_df = pd.read_csv(halo_info_filename, nrows=n_halos)

input_filename = "/home/mhan/data/m000p.full.mpicosmo.59"
output_path = "/Users/mhan/Desktop/Projects/3A_Paraview_GenIO.cdb"
start_time = time.perf_counter()

# create a new 'vtk Gen IO Reader'
m000pfullmpicosmo59 = vtkGenIOReader(registrationName='m000p.full.mpicosmo.59', FileNames=[input_filename])

# Properties modified on m000pfullmpicosmo59
m000pfullmpicosmo59.ShowData = 1

print("done loading data")
# Properties modified on m000pfullmpicosmo59
m000pfullmpicosmo59.Scalar = ''
m000pfullmpicosmo59.Value = ''
m000pfullmpicosmo59.Value2range = ''



# get active view
renderView1 = GetActiveViewOrCreate('RenderView')

# show data in view
m000pfullmpicosmo59Display = Show(m000pfullmpicosmo59, renderView1, 'UnstructuredGridRepresentation')

# trace defaults for the display properties.
m000pfullmpicosmo59Display.Representation = 'Points'

# reset view to fit data
renderView1.ResetCamera(False, 0.9)

# get the material library
materialLibrary1 = GetMaterialLibrary()

# update the view to ensure updated data information
renderView1.Update()

# Properties modified on renderView1
renderView1.UseColorPaletteForBackground = 0
renderView1.Background = [0.0, 0.0, 0.0]

# update the view to ensure updated data information
renderView1.Update()

end_time = time.perf_counter()
elapsed_time = end_time - start_time
print("Time taken for loading:", elapsed_time)


## Start create clipping box for each halo 
for index, row in csv_df.iterrows():
    halo_tag = row['fof_halo_tag']
    halo_count = row['fof_halo_count']
    halo_center = [row['fof_halo_center_x'], row['fof_halo_center_y'], row['fof_halo_center_z']]
    # create a new 'Clip'
    clip1 = Clip(registrationName='Clip1', Input=m000pfullmpicosmo59)

    # toggle interactive widget visibility (only when running from the GUI)
    ShowInteractiveWidgets(proxy=clip1.ClipType)

    # Properties modified on clip1
    clip1.ClipType = 'Box'

    # Properties modified on clip1.ClipType
    clip1.ClipType.Position = [halo_center[0] - radius, halo_center[1] - radius, halo_center[2] - radius]
    clip1.ClipType.Length = [2 * radius, 2 * radius, 2 * radius]

    # show data in view
    clip1Display = Show(clip1, renderView1, 'UnstructuredGridRepresentation')

    # trace defaults for the display properties.
    clip1Display.Representation = 'Points'

    HideInteractiveWidgets(proxy=clip1.ClipType)
    
    # update the view to ensure updated data information
    renderView1.Update()

    # hide data in view
    Hide(m000pfullmpicosmo59, renderView1)

    renderView1.ResetActiveCameraToPositiveX()

    # reset view to fit data
    renderView1.ResetCamera(False, 0.9)
    # get layout
    layout1 = GetLayout()

    #--------------------------------
    # saving layout sizes for layouts

    # layout/tab size in pixels
    layout1.SetSize(2392, 1130)
    
    #-----------------------------------
    # saving camera placements for views

    # current camera placement for renderView1
    renderView1.CameraPosition = [52.615739140195075, 84.0, 127.0]
    renderView1.CameraFocalPoint = halo_center
    renderView1.CameraViewUp = [0.0, 0.0, 1.0]
    renderView1.CameraParallelScale = 3.4641016151377544
    
        
    origin = renderView1.CameraFocalPoint
    r = 10
    for angle_phi in phi:
        for angle_theta in theta:
            # if(angle_theta != 90 and angle_theta != -90):
            # save screenshot
            renderView1.CameraPosition = [origin[0] + r*cos(radians(angle_phi)) * cos(radians(angle_theta)), \
                                            origin[1] + r * sin(radians(angle_phi)) * cos(radians(angle_theta)), \
                                            origin[2] + r*sin(radians(angle_theta))] 
            image_filename = "halo_rank_" + str(index) + "_ts_" + str(ts) + "_phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
            SaveScreenshot(os.path.join(output_path, image_filename), renderView1, ImageResolution=[img_width, img_height])

    # destroy convertToPointCloud1
    Delete(clip1)
    del clip1
    
# destroy m000pfullmpicosmo59
Delete(m000pfullmpicosmo59)
del m000pfullmpicosmo59

total_end_time = time.perf_counter()
total_elapsed_time = total_end_time - total_start_time
print("Time taken for all:", total_elapsed_time)
print("All done!\n") 