# examples/developer/4.visualize.py
from dsi.core import Terminal

terminal_visualize = Terminal()

# Run 3.schema.py so schema_data.db is not empty
terminal_visualize.load_module('backend','Sqlite','back-write', filename='schema_data.db')

terminal_visualize.num_tables()
terminal_visualize.list()

# prints all data from 'input'
terminal_visualize.display("input")

# optional input to specify number of rows from 'input' to print
terminal_visualize.display("input", 2)

# optional input to specify which columns to print
terminal_visualize.display("input", 2, ["sim_id", "state1_density", "state2_density", "initial_timestep", "end_step"])


# prints numerical stats for every table in a backend
terminal_visualize.summary()

# prints numerical stats for only 'input'
terminal_visualize.summary("input")

terminal_visualize.close()