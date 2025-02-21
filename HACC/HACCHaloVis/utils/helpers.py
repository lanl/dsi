import os 
from cdb import *

def match_time_steps(run_directory):
    # haloproperties_ts = []
    #     full_res_ts = []
    with open(os.path.join(run_directory, 'params', 'indat.params'), 'r') as file:
        for line in file:
            splits = line.split(" ")
            if(splits[0] == 'PK_DUMP'):
                haloproperties_ts = sorted([int(t) for t in splits[1:]])
            elif(splits[0] == 'FULL_ALIVE_DUMP'):
                full_res_ts = sorted([int(t) for t in splits[1:]])
    
    return haloproperties_ts, full_res_ts

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    #     return 0
    # else: return 1
    
def get_angles(phi_angle, theta_angle):
    phi = []
    for angle in range(-180, 181, phi_angle):
        phi.append( float(angle) )

    theta = []
    for angle in range(-90, 91, theta_angle):
        theta.append( float(angle) )
    return [phi, theta]

    
# def addEntryToCDB(cdbDatabase, 