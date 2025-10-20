# examples/user/5.update.py
from dsi.dsi import DSI

update_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.find(condition, collection)
find_df = update_dsi.find("state2_density > 5.0", True, True) # Returns matching rows as a DataFrame

update_dsi.display(find_df["dsi_table_name"][0], 5) # display table before update

find_df["new_col"] = 50   # add new column to this DataFrame
find_df["max_timestep"] = 100 # update existing column

#dsi.update(collection, backup)
update_dsi.update(find_df, True) # update the table in the backend

update_dsi.display(find_df["dsi_table_name"][0], 5) # display table after update

update_dsi.close()