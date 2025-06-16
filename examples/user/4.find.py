# examples/user/4.find.py
from dsi.dsi import DSI

find_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.find(value)
find_dsi.find("Jun 2025") # finds the value 2 in all tables

#dsi.find(value, True)
df_list = find_dsi.find("Jun 2025", True) # returns list of DataFrames for tables where 2 is found

find_dsi.close()