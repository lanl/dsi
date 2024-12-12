#Loading using plugins and backends
from dsi.core import Terminal

'''Example access workflow once database has been generated'''

a=Terminal(debug_flag=True)
a.load_module('backend','Sqlite','back-read', filename='wildfire.db')
a.transload()
cnames = a.artifact_handler(interaction_type='get', query = "PRAGMA table_info(wfdata);")
data = a.artifact_handler(interaction_type='get', query = "SELECT * FROM wfdata;")#, isVerbose = True)

# a.artifact_handler(interaction_type="read")
clist = [i[1] for i in cnames]
dlist = list(data)
print(clist)
print(dlist)
# a.unload_module('backend', 'Sqlite', 'back-write')
