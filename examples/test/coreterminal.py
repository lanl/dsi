from dsi.core import Terminal

'''This is an example workflow using core.py'''
a=Terminal(debug=0, backup_db = False, runTable=False)

''' Example uses of loading open DSI readers '''
# a.load_module('plugin','Bueno','reader', filenames=['bueno1.data', 'bueno2.data'])
# a.load_module('plugin','Hostname','reader')

a.load_module('plugin', 'Schema', 'reader', filename="example_schema.json")
# a.load_module('plugin', 'Schema', 'reader', filename="yaml1_schema.json")

a.load_module('plugin', 'YAML1', 'reader', filenames=["student_test1.yml", "student_test2.yml"])
# a.load_module('plugin', 'TOML1', 'reader', filenames=["results.toml", "results1.toml"], target_table_prefix = "results")
# a.load_module('plugin', 'Csv', 'reader', filenames="yosemite5.csv")
# a.load_module('plugin', 'Ensemble', 'reader', filenames="wildfiredata.csv")
# a.load_module('plugin', 'Cloverleaf', 'reader', folder_path="../clover3d/")

# a.load_module('plugin', 'Oceans11Datacard', 'reader', filenames=['../wildfire/wildfire_oceans11.yml', '../pennant/pennant_oceans11.yml'])
# a.load_module('plugin', 'DublinCoreDatacard', 'reader', filenames="../wildfire/wildfire_dublin_core.xml")
# a.load_module('plugin', 'SchemaOrgDatacard', 'reader', filenames="../wildfire/wildfire_schema_org.json")
# a.load_module('plugin', 'GoogleDatacard', 'reader', filenames="../wildfire/wildfire_google.yml")


''' Example uses of loading open DSI writers. Need to call transload() after loading to execute them. '''
# a.load_module('plugin', "Table_Plot", "writer", table_name = "physics", filename = "physics_plot.png", display_cols = ["n", "p"])
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')#, target_table_prefix = "physics")
# a.transload()


''' Example of loading a DSI backend, and its data interactions: ingest, query, notebook, process '''
a.load_module('backend','Sqlite','back-write', filename='data.db')
# a.load_module('backend','DuckDB','back-write', filename='data.db')

a.artifact_handler(interaction_type='ingest')
# data = a.artifact_handler(interaction_type='query', query = "SELECT * FROM runTable;")
# print(data)
# a.artifact_handler(interaction_type="notebook") 
# a.artifact_handler(interaction_type="process")
# print(a.get_current_abstraction)

''' Example of editing a table of data from a backend and overwriting the table'''
# a.display("physics")    # printing table data before updating

# data = a.get_table("physics") # data stored as a DataFrame
# data["new_col"] = 50    # new column to table
# data["a"] = 20          # updating existing column in table

# a.overwrite_table("physics", data)
# a.display("physics")    # has changed data now


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

''' Example of printing table information'''
# a.list()
# a.num_tables()
# a.summary(table_name='physics', num_rows = 3)
# a.display(table_name='physics')

''' Listing available modules that can be loaded (readers, writers, and backends) '''
# a.list_available_modules('plugin')
# # ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

# a.list_available_modules('backend')
# # ['Gufi', 'Sqlite', 'DuckDB', 'HPSS']

''' Listing all loaded modules (writers and backends) '''
# print(a.list_loaded_modules())
# # {'writer': [],
# #  'reader': [],
# #  'back-read': [],
# #  'back-write': []}

''' Example use case: reading data from backend and generating an ER Diagram and table plot from its metadata '''
# a.load_module('backend','Sqlite','back-read', filename='data.db')   
# a.artifact_handler(interaction_type="process")
# a.load_module('plugin', "Table_Plot", "writer", table_name = "physics", filename = "physics")
# a.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')#, target_table_prefix = "physics")
# a.transload()