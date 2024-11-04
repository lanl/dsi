#Loading using plugins and backends
from dsi.core import Terminal

'''This is an example workflow using core.py'''

a=Terminal(debug_flag=False)

# a.load_module('plugin','Bueno','reader', filenames='data/bueno1.data')
# a.load_module('plugin','Hostname','reader')

# a.load_module('plugin', 'Schema', 'reader', filename="data/example_schema.json" , target_table_prefix = "student")
# a.load_module('plugin', 'YAML', 'reader', filenames=["data/student_test1.yml", "data/student_test2.yml"], target_table_prefix = "student")
# a.load_module('plugin', 'TOML', 'reader', filenames=["data/results.toml"], target_table_prefix = "results")

# a.load_module('plugin', "Table_Plot", "writer", table_name = "schema__physics", filename = "schema__physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()

a.load_module('backend','Sqlite','back-write', filename='data/data.db')   
# a.load_module('backend','Parquet','back-write',filename='./data/bueno.pq')

a.artifact_handler(interaction_type='put')
# data = a.artifact_handler(interaction_type='get', query = "SELECT * FROM sqlite_master WHERE type='table';")#, isVerbose = True)
# print(data)

# a.unload_module('backend', 'Sqlite', 'back-write')


# LIST MODULES
# a.list_available_modules('plugin')
# # ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

# a.list_available_modules('backend')
# # ['Gufi', 'Sqlite', 'Parquet', 'SqliteReader]

# print(a.list_loaded_modules())
# # {'writer': [<dsi.plugins.env.Hostname object at 0x7f21232474d0>],
# #  'reader': [<dsi.plugins.env.Bueno object at 0x7f2123247410>],
# #  'back-read': [],
# #  'back-write': []}


# Example use 1
# a.load_module('plugin','Bueno','reader',filenames='data/bueno1.data')
# a.load_module('backend','Sqlite','back-write',filename='data/bueno.db')
# a.transload()
# a.artifact_handler(interaction_type='put')
# data = a.artifact_handler(interaction_type='get', query = "SELECT * FROM sqlite_master WHERE type='table';")#, isVerbose = True)
# print(data)


#Example use 2
# a.load_module('backend','SqliteReader','back-read', filename='data/data.db')   
# a.artifact_handler(interaction_type="read")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()