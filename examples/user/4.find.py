# examples/user/4.find.py
from dsi.dsi import DSI

find_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.search(value)
find_dsi.search("Jun 2025") # searches for the value 'Jun 2025' in all tables

find_df = find_dsi.search("Jun 2025", True) # Returns the first matching table as a DataFrame


#dsi.find(condition, True)
find_dsi.find("state2_density > 5.0") # Finds all rows of one table that match the condition

find_df = find_dsi.find("state2_density > 5.0", True) # Returns matching rows as a DataFrame

find_dsi.close()