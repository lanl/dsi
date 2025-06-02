# examples/user/4.process.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.read.py:
write_dsi = DSI()

#dsi.backend(filename, backend)
write_dsi.backend("data.db")

#dsi.write(filename, writer, table)
write_dsi.write("er_diagram.png", "ER_Diagram")
write_dsi.write("math_table_plot.png", "Table_Plot", "math")
write_dsi.write("math.csv", "Csv_Writer", "math")

write_dsi.close()