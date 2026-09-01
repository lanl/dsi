# examples/developer/8.overwrite.py
from dsi.core import Terminal

terminal_overwrite = Terminal()

# Run 3.schema.py so schema_data.db is not empty
terminal_overwrite.load_module('backend','Sqlite','back-write', filename='schema_data.db')

data = terminal_overwrite.get_table(table_name="input") # data in form of Pandas DataFrame

data["new_column"] = 200    # creating new column
data["end_step"] = 35       # editing existing column

terminal_overwrite.overwrite_table(table_name = "input", collection = data)

terminal_overwrite.display("input", num_rows=5, display_cols= ["sim_id", "state1_density", "state2_density", "end_step", "new_column"])
