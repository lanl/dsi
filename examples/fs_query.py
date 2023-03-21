#from asyncio.windows_events import NULL
import os

from dsi.utils import utils
from dsi.sql.fs import fs

#
#  Query test for previously ingested data
#

datapath = "/usr/projects/dsi_re/data/shell_implosion_1d/"

# Define path of the database
dbpath = "test.db"
store = fs.store(dbpath)

#Control verbosity of debug prints
fs.isVerbose = 1

data_type = fs.data_type()
data_type.name = "filesystem"

# Now try querying the database

# Give me all the files, also a generic SQL query
# Command: ls -lr *
result = store.sqlquery("SELECT * FROM " + str(data_type.name) + " LIMIT 10")
store.export_csv(result, "query.csv")

# Give me the count of all the files
# Command: find . -type f | wc -l 
#result = store.sqlquery("SELECT COUNT(*) FROM " + str(data_type.name))

# Give me one file, and its path
#ffiles = store.query_fname( "case215data_ss_304_t08.cs8.s110.g08.cv7.xvel5.npy")

# Give me files larger than 100mb, in bytes
# Command: find . -type f -size +100M
#sfiles = store.query_fsize(fs.GT, 104857600 )

# Give me files newer than 2020 09 25 5:00pm
# Command: find . -type f -newermt "2020-09-25" \! -newermt "2020-09-25"
#tfiles = store.query_fctime(fs.GT, utils.dateToPosix(2020,9,24,17,00) )

store.close()
print("Done")
