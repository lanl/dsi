#from asyncio.windows_events import NULL
import os
import urllib.request
import sys

#scp jesus@192.168.0.102:D:\Data\wildfire\wildfire.db .
#powershell Set-ExecutionPolicy RemoteSigned

# Temporary until DSI becomes a module
# Path to root dsi folder
sys.path.insert(1, '../')
from dsi.backends.sqlite import Sqlite, DataType
from dsi.backends.filesystem import Filesystem as fs

# Analysis
import matplotlib.pyplot as plt
import numpy as np

#
#  Main Test
#

datapath = "./"

ruser = "jesus"
rserver = "192.168.88.248"

# Define path of the database
dbpath = "wildfire.db"
store = Sqlite(dbpath)

#Control verbosity of debug prints
fs.isVerbose = 1

#List the data in the database
anames = store.get_artifacts()
for name in anames:
    print( name[0] )

# Do a simple query and fine relevant data
data_type = "vision"
result_data = store.sqlquery("SELECT *, MAX(inside_burned_area) AS inside_burned_area FROM " + str(data_type) + " GROUP BY safe_unsafe_fire_behavior")

dataiter = []

# Let's try to actually use the data that's related to the query above
# Find reference between two tables
data_type = "filesystem"
for res in result_data:
    # Trim filename from file entry inside of 'vision'
    fname = os.path.basename(os.path.normpath(res[6]))
    # Relate to filesystem table
    result_fs = store.sqlquery("SELECT * FROM " + str(data_type) + " WHERE file LIKE '%" + fname + "%'")

    # Locate data, see if we need to update it locally
    if os.path.isfile(datapath + fname):
        print("Local:" + datapath + fname)
        # Run hash 

        # Run comparison

    else:
        # Retrieve data from server
        print(result_fs)
        rpath, = result_fs
        rfile = rpath[0]
        os.system('scp -p "%s@%s:%s" "%s" ' % (ruser, rserver, rfile, datapath) )
        print("Remote: " + rfile)

    # Append iterator
    datimage=plt.imread(datapath + fname)
    dataiter.append(datimage)

#store.export_csv(result, "query2.csv")
store.close()


#for img in datimage:
#    print(img.shape)

print("Done")