#Loading using plugins and backends
from dsi.core import Terminal

'''This is an example workflow using core.py'''

a=Terminal(debug_flag=False, run_table_flag=True)

a.load_module('plugin','Bueno','reader', filenames=['data/bueno1.data', 'data/bueno2.data'])
a.load_module('plugin','Hostname','reader')

a.load_module('plugin', 'Schema', 'reader', filename="data/example_schema.json", target_table_prefix = "student")
a.load_module('plugin', 'YAML1', 'reader', filenames=["data/student_test1.yml", "data/student_test2.yml"], target_table_prefix = "student")
a.load_module('plugin', 'TOML1', 'reader', filenames=["data/results.toml", "data/results1.toml"], target_table_prefix = "results")
# a.load_module('plugin', 'MetadataReader1', 'reader', filenames=["data/metadata.json"])

# a.load_module('plugin', "Table_Plot", "writer", table_name = "student__physics", filename = "student__physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()

a.load_module('backend','Sqlite','back-write', filename='data/data.db')
# a.load_module('backend','Parquet','back-write',filename='data/bueno.pq')

a.artifact_handler(interaction_type='write')
# data = a.artifact_handler(interaction_type='process', query = "SELECT * FROM runTable;")#, isVerbose = True)
# print(data)
# a.artifact_handler(interaction_type="inspect")

# data = a.find("data.db", "student")
# data = a.find("data.db", "student", colFlag = True) #return list of col names from the table instead of the whole table
# print(data)

# a.unload_module('backend', 'Sqlite', 'back-write')

# LIST MODULES
# a.list_available_modules('plugin')
# # ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

# a.list_available_modules('backend')
# # ['Gufi', 'Sqlite', 'Parquet']

# print(a.list_loaded_modules())
# # {'writer': [],
# #  'reader': [],
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
# a.load_module('backend','Sqlite','back-read', filename='data/data.db')   
# a.artifact_handler(interaction_type="read")
# a.load_module('plugin', "Table_Plot", "writer", table_name = "student__physics", filename = "student__physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()