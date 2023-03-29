#from asyncio.windows_events import NULL
import os
import urllib.request
import sys

# Temporary until DSI becomes a module
# Path to root dsi folder
sys.path.insert(1, '../')

from dsi.utils import utils
from dsi.sql.fs import fs

#
#  Main Test
#

datapath = "../data/"

# Define path of the database
#dbpath = "shell_implosion_1d.db"
dbpath = "wildfire.db"
store = fs.store(dbpath)

#Control verbosity of debug prints
fs.isVerbose = 1

#List the data in the database
anames = store.get_artifact_list()
for name in anames:
    print( name[0] )

data_type = fs.data_type()
data_type.name = "simulation"

# Now try querying the database

# Give me all the files, also a generic SQL query
# Command: ls -lr *
#result = store.sqlquery("SELECT * FROM " + str(data_type.name) + " LIMIT 10")
#result = store.sqlquery("SELECT * FROM " + str(data_type.name) + " WHERE (safe_unsafe_ignition_pattern like 'safe') AND (does_fire_meet_objectives like 'yes')")
#store.export_csv(result, "query.csv")


#data_type.name = "vision"
#result = store.sqlquery("SELECT FILE FROM " + str(data_type.name) )
# Download the data from the web
#for fname in result:
#    hfname=fname[0].rsplit('/', 1)[-1]
#    urllib.request.urlretrieve(fname[0], hfname)
#    print( "Done: " + str(hfname))


#result = store.sqlquery("SELECT * FROM " + str(data_type.name) + " WHERE (safe_unsafe_fire_behavior like 'safe') AND (inside_burned_area = ( SELECT MAX(inside_burned_area) FROM vision ))")

#result = store.sqlquery("SELECT * FROM " + str(data_type.name) + " WHERE (safe_unsafe_fire_behavior like 'safe') AND inside_burned_area>=(SELECT MAX(inside_burned_area) FROM vision) ORDER BY inside_burned_area DESC")

data_type.name = "vision"
result = store.sqlquery("SELECT *, MAX(inside_burned_area) AS inside_burned_area FROM " + str(data_type.name) + " GROUP BY safe_unsafe_fire_behavior")

store.export_csv(result, "query2.csv")


store.close()
print("Done")
