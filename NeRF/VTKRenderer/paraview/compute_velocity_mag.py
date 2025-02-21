import os 
import numpy as np
import pyvista as pv 

data_path = '/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/'
runs = [f for f in os.listdir(data_path) if f.startswith('VKIN')]
n_halos = 5
for r, run in enumerate(runs):
    current_path = os.path.join(data_path, run, '624')
    for h in range(n_halos):
        halo_path = os.path.join(current_path, "halo_" + str(h) + '.vtu')
        mesh = pv.read(halo_path)
        vx = mesh['vx']
        vy = mesh['vy']
        vz = mesh['vz']
        v_mag = np.sqrt(vx**2 + vy**2 + vz**2)
        mesh['v_mag'] = v_mag
        mesh.save(halo_path)