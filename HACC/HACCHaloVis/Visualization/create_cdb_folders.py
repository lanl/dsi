import os
import time
import sys
sys.path.append('/Users/mhan/Desktop/HACCHaloVis/DBGenerator/')  
sys.path.append('/Users/mhan/Desktop/HACCHaloVis/utils/')    
from database import Database
from helpers import *


if __name__ == "__main__":
    designCDB_path = "/Users/mhan/Desktop/Projects/3A_Paraview.cdb" ## path to save cinema database 
    ## angles for camera views 
    aPhi = 60
    aTheta = 45
    [phi, theta] = get_angles(aPhi, aTheta) 
    
    ## Before creating cinema database find what attributes will be inserted into data.csv in cdb 
    ## load meta database 
    database = Database("./databases/halo_galaxy_5.db")
    ## create the cdb for all runs 
    sql = "SELECT * from runs"
    runs = database.queryTable(sql)
    
    create_directory(designCDB_path)
    designCDB = cdb(designCDB_path)
    designCDB.initialize()
    
    for r, run_item in enumerate(runs):
        runID = run_item['runID']
        # FSN = run_item['FSN']
        # VEL = run_item['VEL']
        # TEXP = run_item['TEXP']
        # BETA = run_item['BETA']
        # SEED = run_item['SEED']
        VKIN = run_item['VKIN']
        EPS = run_item['EPS']
        run_path = run_item['PATH']
        ## create a run cdb database 
        run_folder = run_path.split("/")[-1]
        runCDB_path = os.path.join(designCDB_path, run_folder + ".cdb")
        check = create_directory(runCDB_path)
        runCDB = cdb(runCDB_path)
        if(not check):
            runCDB.initialize()
        else:
            runCDB.read_data_from_file()
        '''
        ## fin all ts for this run 
        sql = f"SELECT ts, full_res_dir FROM files WHERE runID={runID}"
        answers = database.queryTable(sql)
        ts_list = []
            
        for item in answers:
            temp = {k: item[k] for k in item.keys()}
            ts_list.append(temp)

        for t, ts_item in enumerate(ts_list):
            # if(t < n_ts):
            ts = ts_item['ts']
            full_res_dir = ts_item['full_res_dir']
            ## create cdb for ts 
        '''
        ## time step can be requested as above or set a sepecific ts 
        ts = 624
        tsCDB_path = os.path.join(runCDB_path, "ts_" + str(ts) + '.cdb')
        check = create_directory(tsCDB_path)
        tsCDB = cdb(tsCDB_path)
        if(not check):
            tsCDB.initialize()
        else:
            tsCDB.read_data_from_file()
        ## read halo for current runID and ts 
        sql = f"SELECT halo_rank, halo_tag, halo_count, halo_center_x, halo_center_y, halo_center_z FROM halos WHERE runID={runID} AND ts = {ts}"
        answers = database.queryTable(sql)
        halo_list = []
        for i, item in enumerate(answers):
            temp = {k: item[k] for k in item.keys()}
            halo_list.append(temp)
        for h, halo_item in enumerate(halo_list):
            halo_rank = halo_item['halo_rank']
            halo_tag = halo_item['halo_tag']
            halo_count = halo_item['halo_count']
            halo_center_x = halo_item['halo_center_x']
            halo_center_y = halo_item['halo_center_y']
            halo_center_z = halo_item['halo_center_z']
            # create halo cdb database
            haloCDB_path = os.path.join(tsCDB_path, "halo_" + str(halo_rank) + '.cdb')
            check = create_directory(haloCDB_path)
            haloCDB = cdb(haloCDB_path)
            if(not check):
                haloCDB.initialize()
            else:
                haloCDB.read_data_from_file()
            for angle_phi in phi:
                for angle_theta in theta:
                    image_name = "phi_" + str(angle_phi) + '_theta_' + str(angle_theta) + '.png'
                    haloCDB.add_entry({'phi': str(angle_phi), 'theta': str(angle_theta), 'FILE': image_name}) 
                    image_name = "halo_" + str(halo_rank) + ".cdb/" + image_name
                    tsCDB.add_entry({"halo_rank": halo_rank, "halo_tag": halo_tag, "halo_count": halo_count, 'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    image_name = "ts_" + str(ts) + ".cdb/" + image_name
                    runCDB.add_entry({"ts": str(ts), "halo_rank": halo_rank, "halo_tag": halo_tag, "halo_count": halo_count,  "halo_center_x": halo_center_x, "halo_center_y": halo_center_y, "halo_center_z": halo_center_z, 'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    image_name = run_folder + ".cdb/" + image_name
                    # designCDB.add_entry({"runID": str(runID), "FSN": FSN, "VEL": VEL, "TEXP": TEXP, "BETA": BETA, "SEED": SEED, \
                    #                         "ts": str(ts), "halo_rank": halo_rank, "halo_tag": halo_tag, "halo_count": halo_count, \
                    #                         'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
                    designCDB.add_entry({"runID": str(runID), "VKIN": VKIN, "EPS": EPS, \
                                            "ts": str(ts), "halo_rank": halo_rank, "halo_tag": halo_tag, "halo_count": halo_count, \
                                                "halo_center_x": halo_center_x, "halo_center_y": halo_center_y, "halo_center_z": halo_center_z, \
                                            'phi': str(angle_phi), "theta": str(angle_theta), "FILE": image_name})
            haloCDB.finalize()
        tsCDB.finalize()
        runCDB.finalize()
    designCDB.finalize()
    