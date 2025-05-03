# examples/user/4.process.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.ingest.py:
process_dsi = DSI()

process_dsi.backend(filename="data.db", backend_name='Sqlite')

process_dsi.process() # need to call backend() before process() to be able to process data

process_dsi.write(filename="er_diagram.png", writer_name="ER_Diagram")

process_dsi.write(filename="math_table_plot.png", writer_name="Table_Plot", table_name="math")

process_dsi.write(filename="math.csv", writer_name="Csv_Writer", table_name="math")

process_dsi.close()