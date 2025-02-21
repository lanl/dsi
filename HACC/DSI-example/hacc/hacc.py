import os
import pyvista as pv 
import sqlite3


if __name__ == "__main__":
    db_filename = "hacc.db"
    #file directory of all time steps
    fileDir = "/Users/mhan/Desktop/data/sample_test/"
    filenames = sorted([f for f in os.listdir(fileDir) if f.endswith('vtu')])
    print("filenames:", filenames)

    # Read one time step to start 
    
    # attributes = data.point_data

    con = sqlite3.connect("hacc.db")
    cur = con.cursor()


    # with open("metadata.sql", "w") as sql_file:
        # go over each time step 
    for t in range(1):
        filepath = fileDir + filenames[t]
        data = pv.read(filepath)
        # create a high-level info table with vtk_type, n_cells, n_points, xmin, xmax, ymin, ymax, zmin, zmax, n_arrays
        cur.execute(f"CREATE TABLE IF NOT EXISTS hacc_info_{t} (filepath, vtk_type, n_cells, n_points, xmin, xmax, ymin, ymax, zmin, zmax, n_arrays)")
        cur.execute(f"INSERT OR REPLACE INTO hacc_info_{t} VALUES ('{filepath}', 'UnstructuredGrid', {data.n_cells}, {data.n_points}, {data.bounds[0]}, {data.bounds[1]}, {data.bounds[2]}, {data.bounds[3]}, {data.bounds[4]}, {data.bounds[5]}, {data.n_arrays})")
        
        # Read the data attributes
        attribute_names = data.array_names 
        # print("attributes: ", attribute_names)
        # print(data.point_data.get_array('x')[0].dtype)
        
        create_array_names = f"CREATE TABLE IF NOT EXISTS hacc_array_info_{t} ("
        insert_array_types = f"INSERT OR REPLACE INTO hacc_array_info_{t} VALUES ("
        for a in attribute_names:
            create_array_names += f"{a}, "
            type = data.point_data.get_array(a)[0].dtype
            insert_array_types += f"'{type}', "
            # elif(type == "")
        ## remove the last "," and add a ")"
        create_array_names = create_array_names[:-2] + ");\n"
        print(create_array_names)
        insert_array_types = insert_array_types[:-2] + ");\n"
        print(insert_array_types)
        
        # create table for array's type 
        cur.execute(create_array_names)
        cur.execute(insert_array_types)
        
        con.commit()
        


        