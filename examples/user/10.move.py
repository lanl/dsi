from dsi.sync import Sync

# Origin location of data
local_files = "/Users/Shared/dsi/examples/clover3d/"
# Remote Location where database and data will be moved
remote_path = "/Users/Shared/staging/"

# Create Sync type with project database name
s = Sync("clover3d")
s.index(local_files,remote_path,True)
s.copy("cp",True)