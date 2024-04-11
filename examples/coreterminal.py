#Loading using plugins and backends
from dsi.core import Terminal

a=Terminal()

a.list_available_modules('plugin')
# ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

a.load_module('plugin','Bueno','reader',filenames='./data/bueno1.data'')
# Bueno plugin reader loaded successfully.

a.load_module('plugin','Hostname','writer')
# Hostname plugin writer loaded successfully.

a.list_available_modules('backend')
#['Gufi', 'Sqlite', 'Parquet']

#a.load_module('backend','Sqlite','back-end',filenames='./data/bueno.sqlite_db')
#a.load_module('backend','Parquet','back-end',filename='./data/bueno.pq')
              
a.list_loaded_modules()
# {'writer': [<dsi.plugins.env.Hostname object at 0x7f21232474d0>],
#  'reader': [<dsi.plugins.env.Bueno object at 0x7f2123247410>],
#  'front-end': [],
#   'back-end': []}
