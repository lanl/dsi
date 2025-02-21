import sys 
sys.path.append("/home/mhan/genericio/legacy_python/")
import numpy as np 
import genericio as gio

"""
    readHalos function: read user-defined vars from haloproperties file for largest n halos or all halos in the file
    input:
        - inputfilename: path to haloproperties file
        - vars: list of properties to read 
        - n_halos: the number of halos to return 
        - read_all: if true, return for all halos in the file
                    if flase, return the largest n_halos
"""
def readHalos(inputfilename : str, vars :list[str], n_halos: int, read_all :bool = False):
    values = gio.read(inputfilename, vars)
    asc_sorted_halo_size_indices = np.argsort(values[1])
    des_sorted_halo_size_indices = asc_sorted_halo_size_indices[::-1]
    sorted_values = []
    if(not read_all):  
        des_sorted_halo_size_indices = des_sorted_halo_size_indices[0:n_halos]
    for i in range(len(values)):
        temp = values[i][des_sorted_halo_size_indices]
        # array = np.array(values[i][des_sorted_halo_size_indices])
        sorted_values.append(temp)
    
    return sorted_values

  
if __name__ == "__main__":
    inputfilename = "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/128MPC_RUNS_FLAMINGO_DESIGN_3A/FSN_0.2000_VEL_121.060_TEXP_9.226_BETA_0.7839_SEED_2.065e5/analysis/m000p-180.haloproperties"
    vars = ["fof_halo_tag", "fof_halo_count", "fof_halo_center_x", "fof_halo_center_y", "fof_halo_center_z"]
    sorted_array = readHalos(inputfilename, vars, 5)
    print(sorted_array)
    