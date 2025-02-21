import os 
import numpy as np
import pyvista as pv 

if __name__ == "__main__":
    data_path = '/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles_Normalized/'
    runs = [f for f in os.listdir(data_path) if f.startswith('VKIN')]
    n_halos = 5
    two_different_runs = ['VKIN_4400_EPS_2.627', 'VKIN_5800_EPS_3']
    
    for h in range(n_halos):
        v_mag_array = [None] * 16
        for r, run in enumerate(runs):
            rank = h
            current_path = os.path.join(data_path, run, '624')
            if (run in two_different_runs):
                if(h == 0):
                    rank = 4
                elif(h == 1):
                    rank = 0
                elif(h == 2):
                    rank = 1
                elif(h == 3):
                    rank = 2
                else:
                    rank = 3
            halo_path = os.path.join(current_path, "halo_" + str(rank) + '.vtu')
            halo_mesh = pv.read(halo_path)
            ## read the velocity 
            # vx = halo_mesh['vx']
            # vy = halo_mesh['vy']
            # vz = halo_mesh['vz']
            # v_mag = np.sqrt(vx**2 + vy**2 + vz**2)
            v_mag = halo_mesh['v_mag']
            v_mag_array[r] = v_mag
        ## find the minval and maxval 
        min_values = [min(lst) for lst in v_mag_array]
        overall_min = min(min_values)
        max_values = [max(lst) for lst in v_mag_array]
        overall_max = max(max_values)
        print(f"rank: {h} Min: {overall_min} Max: {overall_max}")     