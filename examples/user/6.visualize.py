# examples/user/6.visualize.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.read.py:
visual_dsi = DSI()

#dsi.backend(filename, backend)
visual_dsi.backend("data.db")

visual_dsi.num_tables()
visual_dsi.list()

#dsi.display(table_name, num_rows, column_names)
visual_dsi.display("math") # prints all data from 'math'
visual_dsi.display("math", 2) # optional input to specify number of rows from 'math' to print
visual_dsi.display("math", 2, ['a', 'c', 'e']) # another optional inputs to specify which columns to print

#dsi.summary(table_name, num_rows)
visual_dsi.summary() # prints numerical stats for every table in a backend
visual_dsi.summary("math") # prints numerical stats for only 'math'
visual_dsi.summary("math", 5) # prints numerical stats for only 'math' and prints first 5 rows of the actual table

visual_dsi.close()