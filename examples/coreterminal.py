#Loading using plugins and backends
from dsi.core import Terminal

'''This is an example workflow using core.py'''

a=Terminal(debug=0, backup_db = False, runTable=True)

# a.load_module('plugin','Bueno','reader', filenames=['data/bueno1.data', 'data/bueno2.data'])
# a.load_module('plugin','Hostname','reader')

# a.load_module('plugin', 'Schema', 'reader', filename="data/example_schema.json", target_table_prefix = "student")
a.load_module('plugin', 'YAML1', 'reader', filenames=["data/student_test1.yml", "data/student_test2.yml"], target_table_prefix = "student")
# a.load_module('plugin', 'TOML1', 'reader', filenames=["data/results.toml", "data/results1.toml"], target_table_prefix = "results")
# a.load_module('plugin', 'MetadataReader1', 'reader', filenames=["data/metadata.json"])

# a.load_module('plugin', "Table_Plot", "writer", table_name = "student__physics", filename = "test", display_cols = ["n", "o"])
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()

a.load_module('backend','Sqlite','back-write', filename='data/data.db')

a.artifact_handler(interaction_type='write')
# data = a.artifact_handler(interaction_type='process', query = "SELECT * FROM runTable;")#, isVerbose = True)
# print(data)
# a.artifact_handler(interaction_type="inspect")
# a.artifact_handler(interaction_type="read")
# print(a.active_metadata)

### FIND FUNCTION EXAMPLES
## TABLE match                      - return matching table data
# data = a.find_table("people")
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## COLUMN match                     - return matching column data
# data = a.find_column("a")
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## RANGE match (range = True) - return [min, max] of matching cols
# data = a.find_column("avg", range = True)
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## CELL match                       - return the cells which match the search term
# data = a.find_cell(5.5)
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ROW match (row_return = True)    - return the rows where cells match the search term
# data = a.find_cell(5.9, row = True)
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ALL match                        - return all instances where the search term is found: table, column, cell
# data = a.find("a")
# for val in data:
#     print(val.t_name, val.c_name, val.value, val.row_num, val.type)

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



#Example use 2
# a.load_module('backend','Sqlite','back-read', filename='data/data.db')   
# a.artifact_handler(interaction_type="read")
# a.load_module('plugin', "Table_Plot", "writer", table_name = "student__physics", filename = "student__physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()