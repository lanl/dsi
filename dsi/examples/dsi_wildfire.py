#from asyncio.windows_events import NULL
import os
import sys

# Temporary until DSI becomes a module
# Path to root dsi folder
sys.path.insert(1, '../')

from dsi.utils import utils
from dsi.sql.fs import fs

#
#  Yosemite Wildfire Data Ingestion Test
#

csvpath = "../data/wildfiredata.csv"

dbpath = "wildfire.db"
store = fs.store(dbpath)

#Control verbosity of debug prints
fs.isVerbose = 1

#Use the csv function to insert initial set of data
csvpath = "../data/wildfiredata.csv"
store.put_artifacts_csv(csvpath,"simulation")

#Use the csv function to insert additional artefacts done in post
csvpath = "../data/yosemite5.csv"
store.put_artifacts_csv(csvpath,"vision")

#Query everything
#result = store.sqlquery("SELECT * FROM " + str("vision"))
store.close()
