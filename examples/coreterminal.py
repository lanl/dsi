from dsi.core import Terminal

'''This is an example workflow using core.py'''
a=Terminal(debug=0, backup_db = False, runTable=True)

''' Example uses of loading open DSI readers '''
# a.load_module('plugin','Bueno','reader', filenames=['data/bueno1.data', 'data/bueno2.data'])
# a.load_module('plugin','Hostname','reader')

# a.load_module('plugin', 'Schema', 'reader', filename="data/example_schema.json")
a.load_module('plugin', 'YAML1', 'reader', filenames=["data/student_test1.yml", "data/student_test2.yml"])
# a.load_module('plugin', 'TOML1', 'reader', filenames=["data/results.toml", "data/results1.toml"], target_table_prefix = "results")
# a.load_module('plugin', 'Csv', 'reader', filenames="data/yosemite5.csv")
# a.load_module('plugin', 'Ensemble', 'reader', filenames="data/wildfiredata.csv")

# a.load_module('plugin', 'Oceans11Datacard', 'reader', filenames=['wildfire/wildfire_oceans11.yml', 'pennant/pennant_oceans11.yml'])
# a.load_module('plugin', 'DublinCoreDatacard', 'reader', filenames="wildfire/wildfire_dublin_core.xml")
# a.load_module('plugin', 'SchemaOrgDatacard', 'reader', filenames="wildfire/wildfire_schema_org.json")
# a.load_module('plugin', 'GoogleDatacard', 'reader', filenames="data/template_dc_google.yml")

''' Example uses of loading open DSI writers. Need to call transload() after loading to execute them. '''
# a.load_module('plugin', "Table_Plot", "writer", table_name = "physics", filename = "physics_plot.png", display_cols = ["n", "p"])
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')#, target_table_prefix = "physics")
# a.transload()

''' Example of loading a Sqlite DSI backend, and its data interactions: put (ingest), get (query), inspect (notebook), read (process) '''
a.load_module('backend','Sqlite','back-write', filename='data/data.db')
# a.load_module('backend','DuckDB','back-write', filename='data/data.db')

a.artifact_handler(interaction_type='ingest')
# data = a.artifact_handler(interaction_type='query', query = "SELECT * FROM runTable;")
# print(data)
# a.artifact_handler(interaction_type="notebook") 
# a.artifact_handler(interaction_type="process")
# print(a.get_current_abstraction)

''' Example of printing table information'''
# a.list()
# a.num_tables()
# a.summary(table_name='physics', num_rows = 3)
# a.display(table_name='physics')

''' Example uses of the DSI FIND feature: find_table, find_column, find_cell, find (is a find all) '''
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

''' Listing available modules that can be loaded (readers, writers, and backends) '''
# a.list_available_modules('plugin')
# # ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

# a.list_available_modules('backend')
# # ['Gufi', 'Sqlite', 'Parquet', 'DuckDB']

''' Listing all loaded modules (writers and backends) '''
# print(a.list_loaded_modules())
# # {'writer': [],
# #  'reader': [],
# #  'back-read': [],
# #  'back-write': []}

''' Example use case: reading data from backend and generating an ER Diagram and table plot from its metadata '''
# a.load_module('backend','Sqlite','back-read', filename='data/data.db')   
# a.artifact_handler(interaction_type="process")
# a.load_module('plugin', "Table_Plot", "writer", table_name = "physics", filename = "physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()