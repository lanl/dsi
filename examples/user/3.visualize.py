# examples/user/3.visualize.py
from dsi.dsi import DSI

visual_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

visual_dsi.num_tables()
visual_dsi.list()

#dsi.display(table_name, num_rows, display_cols)
# prints all data from 'input'
visual_dsi.display("input")

# optional input to specify number of rows from 'input' to print
visual_dsi.display("input", 2)

# optional input to specify which columns to print
visual_dsi.display("input", 2, ["sim_id", "state1_density", "state2_density", "initial_timestep", "end_step"])


#dsi.summary(table_name, num_rows)
# prints numerical stats for every table in a backend
visual_dsi.summary()

# prints numerical stats for only 'input'
visual_dsi.summary("input")

visual_dsi.close()