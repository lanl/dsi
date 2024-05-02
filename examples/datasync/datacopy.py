from dsi.core import Sync

#Origin
local_files = "H:/Data/shaped2d/"
#Remote
remote_path = "C:/tmp/"

# Create Sync type with project name
s = Sync("shape2d")
s.copy(local_files,remote_path, True)
