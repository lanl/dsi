# examples/user/7.schema.py
from dsi.dsi import DSI

schema_dsi = DSI("schema_data.db")

# dsi.schema(filename)
schema_dsi.schema("../clover3d/schema.json") # must execute before reading Cloverleaf data

#dsi.read(path, reader)
schema_dsi.read("../clover3d/", 'Cloverleaf')

# ER_Diagram writer requires GraphViz to be installed
# pip install graphviz
try:
    #dsi.write(filename, writer)
    schema_dsi.write("clover_er_diagram.png", "ER_Diagram")
except Exception as e:
    print(f"Error {e} occurred. Do you have graphviz installed?")
    pass

#dsi.display(table_name, num_rows, display_cols)
schema_dsi.display("simulation")
schema_dsi.display("input", ["sim_id", "state1_density", "state2_density", "initial_timestep", "end_step"])
schema_dsi.display("output", ["sim_id", "step", "wall_clock", "average_time_per_cell"])
schema_dsi.display("viz_files")

schema_dsi.close()