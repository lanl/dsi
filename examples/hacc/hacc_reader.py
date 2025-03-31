import os 
import time
from collections import OrderedDict
from dsi.plugins.file_reader import FileReader
import sys 
sys.path.append("/home/mhan/genericio/legacy_python/")
import numpy as np 
import genericio as gio
from pandas import DataFrame
import json 

class HACC(FileReader):
    """
    A Plugin to ingest HACC Suite
    """
    def __init__(self, filename, hacc_suite_path, hacc_run_prefix, target_table_prefix, n_halos = 2, **kwargs):
        super().__init__(filename, **kwargs)
        self.hacc_suite_path = hacc_suite_path
        self.hacc_run_prefix = hacc_run_prefix
        self.schema_file = filename # schema_filename is used for setting primary key and foreign_key
        self.target_table_prefix = target_table_prefix
        self.hacc_data = OrderedDict()
        self.run_table_data = OrderedDict()
        self.file_table_data = OrderedDict()
        self.halo_table_data = OrderedDict()
        self.n_halos = n_halos
        self.halo_vars = ["fof_halo_tag", "fof_halo_count", "fof_halo_center_x", "fof_halo_center_y", "fof_halo_center_z", "sod_halo_radius"]
    
    def match_time_steps(self, run_folder):
        with open(os.path.join(self.hacc_suite_path, run_folder, 'params', 'indat.params'), 'r') as file:
            for line in file:
                splits = line.split(" ")
                if(splits[0] == 'PK_DUMP'):
                    haloproperties_ts = sorted([int(t) for t in splits[1:]])
                elif(splits[0] == 'FULL_ALIVE_DUMP'):
                    full_res_ts = sorted([int(t) for t in splits[1:]])
        
        return haloproperties_ts, full_res_ts
    def readHalos(self, inputfilename, vars, n_halos, read_all = False):
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

    def add_rows(self) -> None:
        total_start = time.perf_counter()
        ## Read run table 
        run_folders = [f for f in os.listdir(self.hacc_suite_path) if f.startswith(self.hacc_run_prefix)][0:1]
        print("run folders", run_folders)
        ## Read all existing time steps 
        haloproperties_ts, full_res_ts = self.match_time_steps(run_folders[0])
        haloproperties_ts = haloproperties_ts[0:2]
        n_ts = len(haloproperties_ts)
        print("num of ts:", n_ts)

        '''
        Runs Table: has run_id, simulation parameters and path to run folder
        '''
        run_temp = []
        '''
        Files Table: has id, run_id, ts, full_snapshot_path, haloproperties_path,
                         bighaloproperties_path, galaxyproperties_path, galaxyparticles_path
        '''
        file_temp = []
        '''
        Halos Table: has id, run_ts_id, halo_rank, halo_tag, halo_count, halo_center_x, halo_center_y, halo_center_z, halo_radius
        '''
        halo_temp = []
        for r, run_folder in enumerate(run_folders):
            ## each entry in run table 
            start_time = time.perf_counter()
            temp_dict = {'run_id': r}
            splits = run_folder.split('_')
            for i in range(len(splits) // 2):
                temp_dict[splits[2 * i]] = float(splits[2 * i + 1])
            temp_dict['run_path'] = os.path.join(self.hacc_suite_path, run_folder)
            run_temp.append(temp_dict)
            print("run ", run_folder)
            for t, halo_ts in enumerate(haloproperties_ts):
                print("ts:", halo_ts)
                index = r * n_ts + t
                full_snapshot_path = ""
                new_simulation_folder_structure = False
                if (halo_ts in full_res_ts):
                    ## read num_elems and num_variables from genericIO file 
                    ## the folder structure for the new simulation is different 
                    check_path = os.path.join(self.hacc_suite_path, run_folder, 'output/full_snapshots')
                    if (os.path.exists(check_path)):
                        new_simulation_folder_structure = True
                        full_snapshot_path = 'output/full_snapshots/step_' + str(halo_ts) + '/m000p.full.mpicosmo.' + str(halo_ts)
                    else:
                        full_snapshot_path = 'output/m000p.full.mpicosmo.' + str(halo_ts)
                if new_simulation_folder_structure:
                    haloproperties_path = 'analysis/haloproperties/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.haloproperties'
                    bighaloparticles_path = 'analysis/bighaloparticles/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.bighaloparticles'
                    galaxyproperties_path = 'analysis/galaxyproperties/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.galaxyproperties'
                    galaxyparticles_path = 'analysis/galaxyparticles/step_' + str(halo_ts) + '/m000p-' + str(halo_ts) + '.galaxyparticles'
                else:
                    haloproperties_path = 'analysis/m000p-' + str(halo_ts) + '.haloproperties'
                    bighaloparticles_path = 'analysis/m000p-' + str(halo_ts) + '.bighaloparticles'
                    galaxyproperties_path = 'analysis/m000p-' + str(halo_ts) + '.galaxyproperties'
                    galaxyparticles_path = 'analysis/m000p-' + str(halo_ts) + '.galaxyparticles'
                temp_dict = {"key": index, "run_id": r, "ts": halo_ts, "full_snapshot_path": full_snapshot_path, \
                            "haloproperties_path": haloproperties_path, "bighaloparticles_path": bighaloparticles_path,\
                            "galaxyproperties_path": galaxyproperties_path, "galaxyparticles_path": galaxyparticles_path}
                file_temp.append(temp_dict)
                print("start reading halos")
                h_start_time = time.perf_counter()
                halo_values = self.readHalos(os.path.join(os.path.join(self.hacc_suite_path, run_folder), haloproperties_path), self.halo_vars, self.n_halos)
                h_end_time = time.perf_counter()
                print(f"reading halo from haloproperties file {(h_end_time - h_start_time):.4f} seconds")
                for h in range(self.n_halos):
                    # print("halo:", h)
                    halo_index = r  * (self.n_halos * n_ts) + t * self.n_halos + h
                    temp_dict = {'key': halo_index, 'run_id': r, "ts": halo_ts,
                                'halo_rank': h,  'halo_tag': int(halo_values[0][h]), 'halo_count': int(halo_values[1][h]), \
                                'halo_center_x': float(halo_values[2][h]), 'halo_center_y': float(halo_values[3][h]), 'halo_center_z': float(halo_values[4][h]), 'halo_radius': float(halo_values[5][h])}
                    halo_temp.append(temp_dict)
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            print(f"Execution time for each run: {execution_time:.4f} seconds")
        print("DONE COLLECT META")
        run_df = DataFrame(run_temp)
        file_df = DataFrame(file_temp)
        halo_df = DataFrame(halo_temp)
        self.run_table_data = OrderedDict(run_df.to_dict(orient='list'))
        self.file_table_data = OrderedDict(file_df.to_dict(orient='list'))
        self.halo_table_data = OrderedDict(halo_df.to_dict(orient='list'))
        # print("run table:", self.run_table_data)
        self.hacc_data["hacc__runs"] = self.run_table_data
        self.hacc_data["hacc__files"] = self.file_table_data
        self.hacc_data['hacc__halos'] = self.halo_table_data

        self.hacc_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
        with open(self.schema_file, 'r') as fh:
            schema_content = json.load(fh)
            
            for tableName, tableData in schema_content.items():
                if self.target_table_prefix is not None:
                    tableName = self.target_table_prefix + "__" + tableName
                    print("tableName:", tableName)
                print("tableData", tableData)
                pkList = []
                for colName, colData in tableData["foreign_key"].items():
                    print("colData", colData)
                    if self.target_table_prefix is not None:
                        colData[0] = self.target_table_prefix + "__" + colData[0]
                    self.hacc_data["dsi_relations"]["primary_key"].append((colData[0], colData[1]))
                    self.hacc_data["dsi_relations"]["foreign_key"].append((tableName, colName))

                if "primary_key" in tableData.keys():
                    pkList.append((tableName, tableData["primary_key"]))
            
            for pk in pkList:
                if pk not in self.hacc_data["dsi_relations"]["primary_key"]:
                    self.hacc_data["dsi_relations"]["primary_key"].append(pk)
                    self.hacc_data["dsi_relations"]["foreign_key"].append((None, None))
        total_end0 = time.perf_counter()
        execution_time = total_end0 - total_start
        print(f"Execution time before set schema: {execution_time:.4f} seconds")
        self.set_schema_2(self.hacc_data)
        total_end1= time.perf_counter()
        execution_time = total_end1 - total_start
        print(f"Execution time for setting schema: {execution_time:.4f} seconds")