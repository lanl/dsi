# examples/user/4.find.py
from dsi.dsi import DSI

find_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.find(value)
find_dsi.find("Jun 2025") # finds the value 2 in all tables

#dsi.find(value, True)
find_df = find_dsi.find("Jun 2025", True) # Returns the first matching table as a DataFrame

#dsi.find(condition, True)
find_df = find_dsi.find("state2_density > 5.0", True) # Returns matching rows as a DataFrame

find_dsi.close()