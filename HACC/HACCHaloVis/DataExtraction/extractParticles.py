import sys 
sys.path.append("/home/mhan/genericio/legacy_python/")
import genericio as gio
import os
import numpy as np 
from pyevtk.hl import pointsToVTK 
import math

'''
    Extract halo particles from bighaloparticles file with halo_tag
    Return: x, y, x, attributes: {"phi", "vx", "vy", "vz"}
'''
def extractFromBighaloparticles(bighaloparticles_path, halo_tag, fof_halo_center_x, fof_halo_center_y, fof_halo_center_z, halo_radius, vars):

    # results = gio.read(bighaloparticles_path, vars)
    ## Using center and halo_radius to define a region for faster processing
    center = [fof_halo_center_x, fof_halo_center_y, fof_halo_center_z]
    extents = [round(center[0]-halo_radius), round(center[0]+halo_radius),   
               round(center[1]-halo_radius), round(center[1]+halo_radius),   
               round(center[2]-halo_radius), round(center[2]+halo_radius)]
    ## find the ranks 
    num_ranks, ranks = gio.get_ranks_in(bighaloparticles_path, extents)
    selected_x = []
    selected_y = []
    selected_z = []
    selected_phi = []
    selected_vx = []
    selected_vy = []
    selected_vz = []
    for r in ranks:
        print("rank", r)
        results = gio.read(bighaloparticles_path, vars, r)
        fof_halo_tag_array = results[4]
        # print(fof_halo_tag_array)
        indices = np.where(fof_halo_tag_array == halo_tag)[0]
        _x = np.array(results[0])[indices] 
        _y = np.array(results[1])[indices] 
        _z = np.array(results[2])[indices] 
        _phi = np.array(results[3][indices])
        _vx = np.array(results[5][indices])
        _vy = np.array(results[6][indices])
        _vz = np.array(results[7][indices])
        selected_x.extend(_x)
        selected_y.extend(_y)
        selected_z.extend(_z)
        selected_phi.extend(_phi)
        selected_vx.extend(_vx)
        selected_vy.extend(_vy)
        selected_vz.extend(_vz)
    selected_x = np.array(selected_x)
    selected_y = np.array(selected_y)
    selected_z = np.array(selected_z)
    attributes = {"phi" : np.array(selected_phi), "vx": np.array(selected_vx), "vy": np.array(selected_vy), "vz": np.array(selected_vz)}
    return selected_x, selected_y, selected_z, attributes
    # if(len(selected_x) != 0):
    #     pointsToVTK(output_filename, selected_x, selected_y, selected_z, data = {"phi" : selected_phi, "vx": selected_vx, "vy": selected_vy, "vz": selected_vz})

'''
    Extract halo particles in a region from bighaloparticles file 
    Return: x, y, x, attributes: {"phi", "vx", "vy", "vz"}
'''

def extractFromBighaloparticlesByRegion(bighaloparticles_path, fof_halo_center_x, fof_halo_center_y, fof_halo_center_z, halo_radius, vars):
    center = [fof_halo_center_x, fof_halo_center_y, fof_halo_center_z]
    extents = [round(center[0]-halo_radius), round(center[0]+halo_radius),   
               round(center[1]-halo_radius), round(center[1]+halo_radius),   
               round(center[2]-halo_radius), round(center[2]+halo_radius)]
    box_min = np.array( [center[0]-halo_radius, center[1]-halo_radius, center[2]-halo_radius])
    box_max = np.array( [center[0]+halo_radius, center[1]+halo_radius, center[2]+halo_radius])

    num_ranks, ranks = gio.get_ranks_in(bighaloparticles_path, extents)
    selected_x = []
    selected_y = []
    selected_z = []
    selected_phi = []
    selected_vx = []
    selected_vy = []
    selected_vz = []
    for r in ranks:
        results = gio.read(bighaloparticles_path, vars, r)
        points = np.stack([results[0],results[1], results[2]], 1)
        # find indices of points that are in the box bounds
        indices = np.all((points >= box_min) & (points <= box_max), axis=1)
        _x = np.array(results[0])[indices] 
        _y = np.array(results[1])[indices] 
        _z = np.array(results[2])[indices] 
        _phi = np.array(results[3][indices])
        _vx = np.array(results[5][indices])
        _vy = np.array(results[6][indices])
        _vz = np.array(results[7][indices])
        selected_x.extend(_x)
        selected_y.extend(_y)
        selected_z.extend(_z)
        selected_phi.extend(_phi)
        selected_vx.extend(_vx)
        selected_vy.extend(_vy)
        selected_vz.extend(_vz)
    selected_vx = np.array(selected_vx)
    selected_vy = np.array(selected_vy)
    selected_vz = np.array(selected_vz)
    selected_x = np.array(selected_x)
    selected_y = np.array(selected_y)
    selected_z = np.array(selected_z)
    selected_vmag = np.sqrt(selected_vx**2 + selected_vy**2 + selected_vz**2)
    selected_phi = np.array(selected_phi)
    attributes = {"phi" : selected_phi, "vx": selected_vx, "vy": selected_vy, "vz": selected_vz, "v_mag": selected_vmag}
    return selected_x, selected_y, selected_z, attributes

'''
    Extract galaxy particles from galaxyparticles file with halo_tag
    Return: x, y, x, attributes: {"phi", "vx", "vy", "vz"}
'''
def extractFromGalaxyparticles(galaxyproperties_path, galaxyparticles_path, vars, halo_tag, fof_halo_center_x, fof_halo_center_y, fof_halo_center_z, halo_radius):
    center = [fof_halo_center_x, fof_halo_center_y, fof_halo_center_z]
    extents = [round(center[0]-halo_radius), round(center[0]+halo_radius),   
               round(center[1]-halo_radius), round(center[1]+halo_radius),   
               round(center[2]-halo_radius), round(center[2]+halo_radius)]
    # box_min = np.array( [center[0]-halo_radius, center[1]-halo_radius, center[2]-halo_radius])
    # box_max = np.array( [center[0]+halo_radius, center[1]+halo_radius, center[2]+halo_radius])

    num_ranks, ranks = gio.get_ranks_in(galaxyproperties_path, extents)
    # selected_gal_tag = []
    selected_x = []
    selected_y = []
    selected_z = []
    # selected_rho = []
    selected_mass = []
    for r in ranks:
        results = gio.read(galaxyproperties_path, vars, r)
        gal_tag = np.array(results[0])
        gal_fof_halo_tag = np.array(results[2])
        gal_mass = np.array(results[1])
        gal_indices = np.where(gal_fof_halo_tag == halo_tag)[0] ## a list of index where the galaxy belong to this halo 
        _gal_tag = np.array(gal_tag)[gal_indices]
        _gal_mass = gal_mass[gal_indices]
        # selected_gal_tag.extend(_gal_tag)
    
    
        values = gio.read(galaxyparticles_path, ['x', 'y', 'z', 'gal_tag'], r)

        gal_tag_array = values[3]
        for c, current_tag in enumerate(_gal_tag):
            current_mass = _gal_mass[c]
            indices = np.where(gal_tag_array == current_tag)[0]
            _x = np.array(values[0])[indices] 
            _y = np.array(values[1])[indices] 
            _z = np.array(values[2])[indices] 
            # _rho = np.array(values[4][indices])
            # _mass = np.array(values[5][indices])
            _gal_mass_arrray = np.full_like(_x, current_mass)
            selected_x.extend(_x)
            selected_y.extend(_y)
            selected_z.extend(_z)
            # selected_rho.extend(_rho)
            selected_mass.extend(_gal_mass_arrray)
    selected_x = np.array(selected_x)
    selected_y = np.array(selected_y)
    selected_z = np.array(selected_z)
    attributes = {"mass": np.array(selected_mass)}
    return selected_x, selected_y, selected_z, attributes
    # if(len(_x) != 0):
    #     pointsToVTK(output_filename, selected_x, selected_y, selected_z, data = {"rho" : selected_rho, "mass": selected_mass})


def extractFromGalaxyparticlesByRegion(galaxyproperties_path, galaxyparticles_path, vars, halo_tag, fof_halo_center_x, fof_halo_center_y, fof_halo_center_z, halo_radius):
    center = [fof_halo_center_x, fof_halo_center_y, fof_halo_center_z]
    extents = [round(center[0]-halo_radius), round(center[0]+halo_radius),   
               round(center[1]-halo_radius), round(center[1]+halo_radius),   
               round(center[2]-halo_radius), round(center[2]+halo_radius)]
    box_min = np.array( [center[0]-halo_radius, center[1]-halo_radius, center[2]-halo_radius])
    box_max = np.array( [center[0]+halo_radius, center[1]+halo_radius, center[2]+halo_radius])

    num_ranks, ranks = gio.get_ranks_in(galaxyproperties_path, extents)
    # selected_gal_tag = []
    selected_x = []
    selected_y = []
    selected_z = []
    # selected_rho = []
    selected_mass = []
    for r in ranks:
        results = gio.read(galaxyproperties_path, vars, r)
        gal_tag = np.array(results[0])
        gal_fof_halo_tag = np.array(results[2])
        gal_mass = np.array(results[1])
        gal_indices = np.where(gal_fof_halo_tag == halo_tag)[0] ## a list of index where the galaxy belong to this halo 
        _gal_tag = np.array(gal_tag)[gal_indices]
        _gal_mass = gal_mass[gal_indices]
        # selected_gal_tag.extend(_gal_tag)
    
    
        values = gio.read(galaxyparticles_path, ['x', 'y', 'z'], r)

        for c, current_tag in enumerate(_gal_tag):
            current_mass = _gal_mass[c]
            # indices = np.where(gal_tag_array == current_tag)[0]
            points = np.stack([values[0],values[1], values[2]], 1)
            indices = np.all((points >= box_min) & (points <= box_max), axis=1)
            _x = np.array(values[0])[indices] 
            _y = np.array(values[1])[indices] 
            _z = np.array(values[2])[indices] 
            # _rho = np.array(values[4][indices])
            # _mass = np.array(values[5][indices])
            _gal_mass_arrray = np.full_like(_x, current_mass)
            selected_x.extend(_x)
            selected_y.extend(_y)
            selected_z.extend(_z)
            # selected_rho.extend(_rho)
            selected_mass.extend(_gal_mass_arrray)
    selected_x = np.array(selected_x)
    selected_y = np.array(selected_y)
    selected_z = np.array(selected_z)
    attributes = {"mass": np.array(selected_mass)}
    return selected_x, selected_y, selected_z, attributes
    # if(len(_x) != 0):
    #     pointsToVTK(output_filename, selected_x, selected_y, selected_z, data = {"rho" : selected_rho, "mass": selected_mass})


def saveToVTK(output_filename, x, y, z, attributes):
    pointsToVTK(output_filename, x, y, z, data = attributes)
    
    
if __name__ == "__main__":
    bighaloparticles_path = "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM/VKIN_6267_EPS_0.947/analysis/bighaloparticles/step_624/"
    bighaloparticles_path = os.path.join(bighaloparticles_path, "m000p-624.bighaloparticles")
    fof_halo_center_x = 51.991600036621094
    fof_halo_center_y = 16.191099166870117
    fof_halo_center_z = 243.48196411132812
    halo_radius = 10
    vars = ['x', 'y', 'z', 'phi', 'fof_halo_tag', 'vx', 'vy', 'vz']
    [x, y, z, attributes] = extractFromBighaloparticlesByRegion(bighaloparticles_path, fof_halo_center_x, fof_halo_center_y, fof_halo_center_z, halo_radius, vars)
    if(len(x) != 0):
        saveToVTK("./halo_624_radius_10", x, y, z, attributes)