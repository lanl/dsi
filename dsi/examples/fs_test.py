#from asyncio.windows_events import NULL
import os

from dsi.utils import utils
from dsi.sql.fs import fs

#
#  Data Ingestion test defining a schema for open shell implosion data
#

datapath = "/usr/projects/dsi_re/data/shell_implosion_1d/"

# Filecrawl and save file locations
file_list = utils.dircrawl(datapath)
#print("The complete set of files are ", file_list)

# populate st_list to hold all filesystem attributes
st_list = []
# Do a quick validation of group permissions
for file in file_list:
  #utils.isgroupreadable(file) # quick test
  filepath = os.path.join(datapath, file)
  st = os.stat(filepath)
  #print(os.stat(filepath).st_size)
  st_list.append(st)

# Peak at what st_list looks like
#print(st_list)
#print(st_list[1].st_size)
#print(utils.posixToDate(st_list[1].st_ctime))

# Define path of the database
#dbpath = "shell_implosion_1d.db"
dbpath = "test.db"
store = fs.store(dbpath)

#Control verbosity of debug prints
fs.isVerbose = 1

data_type = fs.data_type()
#Begin the driver test
data_type.name = "filesystem"
data_type.properties["file"] = fs.STRING
data_type.properties["st_mode"] = fs.DOUBLE
data_type.properties["st_ino"] = fs.DOUBLE
data_type.properties["st_dev"] = fs.DOUBLE
data_type.properties["st_nlink"] = fs.DOUBLE
data_type.properties["st_uid"] = fs.DOUBLE
data_type.properties["st_gid"] = fs.DOUBLE
data_type.properties["st_size"] = fs.DOUBLE
data_type.properties["st_atime"] = fs.DOUBLE
data_type.properties["st_mtime"] = fs.DOUBLE
data_type.properties["st_ctime"] = fs.DOUBLE
#print(data_type.properties)
store.put_artifact_type(data_type)

artifact = fs.artifact()
for file,st in zip(file_list,st_list):
    artifact.properties["file"] = str(file)
    artifact.properties["st_mode"] = st.st_mode
    artifact.properties["st_ino"] = st.st_ino
    artifact.properties["st_dev"] = st.st_dev
    artifact.properties["st_nlink"] = st.st_nlink
    artifact.properties["st_uid"] = st.st_uid
    artifact.properties["st_gid"] = st.st_gid
    artifact.properties["st_size"] = st.st_size
    artifact.properties["st_atime"] = st.st_atime
    artifact.properties["st_mtime"] = st.st_mtime
    artifact.properties["st_ctime"] = st.st_ctime
    #print(artifact.properties)
    store.put_artifacts(artifact)

print("Data ingestion complete")
# Now try querying the database

# Give me all the files, also a generic SQL query
# Command: ls -lr *
#result = store.sqlquery("SELECT * FROM " + str(data_type.name))

# Give me files larger than 100mb, in bytes
# Command: find . -type f -size +100M
#sfiles = store.query_fsize(fs.GT, 104857600 )

# Give me files newer than 2020 09 25 5:00pm
# Command: find . -type f -newermt "2020-09-25" \! -newermt "2020-09-25"
#tfiles = store.query_fctime(fs.GT, utils.dateToPosix(2020,9,24,17,00) )

store.close()
print("Done")
