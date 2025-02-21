import numpy as np 
import pyvista as pv 
import os 

def normalize_list(lst, min_val, max_val):
    normalized_lst = [(x - min_val) / (max_val - min_val) for x in lst]
    return normalized_lst

if __name__ == "__main__":
    data_path = '/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles/'
    save_path = '/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/2PARAM_Halo_Galaxy_Particles_Normalized/'
    runs = [f for f in os.listdir(data_path) if f.startswith('VKIN')]
    
    two_different_runs = ['VKIN_4400_EPS_2.627', 'VKIN_5800_EPS_3']
    n_halos = 5
    for h in range(n_halos):
        print("current halo rank: ", h)
        rank_list = []
        rank = h
        same_halo_data = [None] * 16
        for r, run in enumerate(runs):
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
                print("current run has different halo rank: ", rank)
            rank_list.append(rank)
            halo_path = os.path.join(current_path, "halo_" + str(rank) + '.vtu')
            same_halo_data[r] = pv.read(halo_path)
        
        ## normalize the halo data 
        vars = ['phi', 'vx', 'vy', 'vz', 'v_mag']
        # vars = ['mass']
        for v, var in enumerate(vars):
            data_array = [None] * 16
            for h, halo_data in enumerate(same_halo_data):
                data = np.array(halo_data[var])
                data_array[h] = data
            min_values = [min(lst) for lst in data_array]
            overall_min = min(min_values)
            max_values = [max(lst) for lst in data_array]
            overall_max = max(max_values)
            # print("min, max", overall_min, overall_max)
            ## normalize the data array 
            for d, data in enumerate(data_array):
                result = normalize_list(data, overall_min, overall_max)
                mesh = same_halo_data[d]
                mesh[var] = result
                current_save_path = os.path.join(save_path, runs[d], '624')
                # print(f'save {d} data array in {current_save_path}')
                mesh.save(os.path.join(current_save_path, 'halo_' + str(rank_list[d]) + '.vtu'))
                

                
            
            
        
            
        
        
        
        
        








        
# ## find the tag and rank 
# tag = halo_file.split('_')[1]
# rank = halo_file.split('_')[3][0]
# galaxy_file = "galaxy_tag_" + tag + "_rank_" + rank + '.vtu'
# halo_path = os.path.join(current_path, halo_file)
# galaxy_path = os.path.join(current_path, galaxy_file)
# new_halo_path = os.path.join(current_path, "halo_" + rank + '.vtu')
# new_galaxy_path = os.path.join(current_path, "galaxy_" + rank + '.vtu')
# os.rename(halo_path, new_halo_path)
# os.rename(galaxy_path, new_galaxy_path)