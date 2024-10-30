#Loading using plugins and backends
from dsi.core import Terminal

'''This is an example workflow using core.py'''

a=Terminal(debug_flag=True)

# a.list_available_modules('plugin')
# ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

a.load_module('plugin','Bueno','reader',filenames='data/bueno1.data')
# Bueno plugin reader loaded successfully.

# a.load_module('plugin','Hostname','writer')
# Hostname plugin writer loaded successfully.

# a.list_available_modules('backend')
# ['Gufi', 'Sqlite', 'Parquet']

a.load_module('plugin', 'YAML', 'reader', filenames=["data/student_test1.yml", "data/student_test2.yml"], target_table_prefix = "student")
#a.load_module('plugin', 'YAML', 'reader', filenames=["data/cmf.yml", "data/cmf.yml"], target_table_name = "cmf")

# print(a.active_metadata)
a.load_module('plugin', 'TOML', 'reader', filenames=["data/results.toml"], target_table_prefix = "results")
# print(a.active_metadata)
a.load_module('backend','Sqlite','back-end', filename='data/data.db')   
#a.load_module('backend','Sqlite','back-end', filename='data/data2.db')   
# a.load_module('backend','Parquet','back-end',filename='./data/bueno.pq')

#a.load_module('plugin', "Table_Plot", "writer", table_name = "schema_physics", filename = "schema_physics")

a.transload()
a.artifact_handler(interaction_type='put')
# a.list_loaded_modules()
# {'writer': [<dsi.plugins.env.Hostname object at 0x7f21232474d0>],
#  'reader': [<dsi.plugins.env.Bueno object at 0x7f2123247410>],
#  'front-end': [],
#   'back-end': []}


# Example use
# a.load_module('plugin','Bueno','reader',filenames='data/bueno1.data')
# a.load_module('backend','Sqlite','back-end',filename='data/bueno.db')
# a.transload()
# a.artifact_handler(interaction_type='put')
data = a.artifact_handler(interaction_type='get', query = "SELECT * FROM sqlite_master WHERE type='table';")#, isVerbose = True)
#CAN PRINT THE DATA OUTPUT
# print(data)