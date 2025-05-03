# examples/user/6.visualize.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.ingest.py:
visual_dsi = DSI()

visual_dsi.backend(filename="data.db", backend_name='Sqlite') 

visual_dsi.num_tables() # need to call backend() before to get number of tables

visual_dsi.list() # need to call backend() before to list all tables and their dimensions

visual_dsi.display(table_name="math") # need to call backend() before to print all data from 'math'
visual_dsi.display(table_name="math", num_rows=2, display_cols=['a', 'c', 'e']) # extra inputs to specify num rows and which columns to print

visual_dsi.summary() # need to call backend() before to print numerical stats from every table in a backend
visual_dsi.summary(table_name="math") # prints numerical stats for only 'math'
visual_dsi.summary(table_name="math", num_rows=5) # prints numerical stats for only 'math' and prints first 5 rows of the actual table

visual_dsi.close()