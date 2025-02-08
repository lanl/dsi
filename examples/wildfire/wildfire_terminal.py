#Loading using plugins and backends
from dsi.core import Terminal

'''Example access workflow once database has been generated'''

a=Terminal(debug=0)
a.load_module('backend','Sqlite','back-read', filename='wildfire.db')

data_object = a.find_table('wfdata')[0]
cnames = data_object.c_name
data = data_object.value
clist = cnames

# cnames = a.artifact_handler(interaction_type='get', query = "PRAGMA table_info(wfdata);")
# data = a.artifact_handler(interaction_type='get', query = "SELECT * FROM wfdata;")#, isVerbose = True)
# clist = [i[1] for i in cnames]

table = [clist] + [list(row) for row in data]

print(table)
a.unload_module('backend', 'Sqlite', 'back-read')