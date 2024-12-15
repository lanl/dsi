from dsi.core import Sync

#Origin
local_files = "H:/Data/Scratch/shaped2d/"
#Remote
remote_path = "H:/tmp/"

# Create Sync type with project name
s = Sync("shape2d")
s.copy(local_files,remote_path, True)
