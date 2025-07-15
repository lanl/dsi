from dsi.core import Sync

#Origin
local_files = "/Users/Shared/dev/dsi/examples/clover3d/"
#Remote
remote_path = "/Users/Shared/staging/"

# Create Sync type with project name
s = Sync("clover3d")
s.index(local_files,remote_path,True)
s.copy("copy",True)
