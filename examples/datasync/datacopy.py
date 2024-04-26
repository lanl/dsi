from dsi.core import Sync


local_files = "H:/Data/shapedCharge2d/"
remote_path = "C:/tmp/"


s = Sync("shape2d")
s.copy(local_files,remote_path)
