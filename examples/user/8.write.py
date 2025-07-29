# examples/user/8.write.py
from dsi.dsi import DSI

write_dsi = DSI("schema_data.db") # Assuming schema_data.db has data from 7.schema.py:

#dsi.write(filename, writer, table)
write_dsi.write("er_diagram.png", "ER_Diagram")

write_dsi.write("input_table_plot.png", "Table_Plot", "input")

write_dsi.write("input.csv", "Csv_Writer", "input")

write_dsi.close()