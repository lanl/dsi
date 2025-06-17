# examples/user/5.update.py
from dsi.dsi import DSI

update_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.find(value, collection)
find_list = update_dsi.find(2, True) # list of DataFrames for tables where 2 is found
for table in find_list:
    update_dsi.display(table["dsi_table_name"][0], 5) # display table before update

    table["new_col"] = 50   # add new column to this DataFrame
    table["timestep"] = 100 # update existing column for some DataFrames

#dsi.update(collection)
update_dsi.update(find_list) # update all tables in the list
update_dsi.update(find_list[0]) # update only first table in the list

for table in find_list:
    update_dsi.display(table["dsi_table_name"][0], 5) # display table after update

update_dsi.close()