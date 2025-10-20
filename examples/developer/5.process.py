# examples/developer/5.process.py
from dsi.core import Terminal

terminal_process = Terminal()

# Run 3.schema.py so schema_data.db is not empty
terminal_process.load_module('backend','Sqlite','back-read', filename='schema_data.db')   
terminal_process.artifact_handler(interaction_type="process")

terminal_process.load_module('plugin', "Table_Plot", "writer", table_name = "output", filename = "output_plot.png")

terminal_process.load_module('plugin', "Csv_Writer", "writer", table_name = "input", filename = "input.csv")

# ER_Diagram writer requires GraphViz to be installed
# pip install graphviz
terminal_process.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')

# After loading a plugin WRITER, need to call transload() to execute it
terminal_process.transload()