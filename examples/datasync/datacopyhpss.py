from dsi.dsi import DSI
from dsi.core import HPSSSync

project = "clover3d"
dbpath = project + '.db'
store = DSI(filename=dbpath, backend_name= "Sqlite")
store.read("../clover3d/", 'Cloverleaf')

#Origin
local_files = ["/users/hng/dsi/examples/clover3d/"]
#Remote
remote_path = "/hpss/hng"

# Create Sync type with project name
s = HPSSSync("clover3d")
s.index(local_files,remote_path,"clover3d.tar.gz")
s.copy("copy",True)
