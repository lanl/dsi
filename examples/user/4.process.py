# examples/user/4.process.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.ingest.py:
process_dsi = DSI()

#dsi.backend(filename, backend)
process_dsi.backend("data.db")

process_dsi.process() # need to call backend() before process() to be able to process data

#dsi.write(filename, writer, table)
process_dsi.write("er_diagram.png", "ER_Diagram")
process_dsi.write("math_table_plot.png", "Table_Plot", "math")
process_dsi.write("math.csv", "Csv_Writer", "math")

process_dsi.close()