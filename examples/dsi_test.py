from dsi.dsi import DSI

'''This is an example workflow using core.py'''
test = DSI()

''' Printing all valid readers, writers, backends '''
# test.list_backends()
# test.list_readers()
# test.list_writers()


''' Example of loading a Sqlite DSI backend'''
test.backend("data.db", backend_name="Sqlite")
# test.backend("data.duckdb", backend_name="DuckDB")


''' Example uses of loading open DSI readers '''
# test.schema(filename="data/example_schema.json")
test.read(filenames=["data/student_test1.yml", "data/student_test2.yml"], reader_name='YAML1')
# test.read(filenames=["data/results.toml", "data/results1.toml"], reader_name='TOML1')
# test.read(filenames="data/yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
# test.read(filenames="data/wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
# test.read(filenames=['data/bueno1.data', 'data/bueno2.data'], reader_name='Bueno')

# test.read(filenames=['wildfire/wildfire_oceans11.yml', 'pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
# test.read(filenames="wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
# test.read(filenames="wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
# test.read(filenames="data/template_dc_google.yml", reader_name='GoogleDatacard')


''' Example uses of loading open DSI writers. '''
# test.write(filename="er_diagram.png", writer_name="ER_Diagram")
# test.write(filename="physics_plot.png", writer_name="Table_Plot", table_name="physics")
# test.write(filename="physics.csv", writer_name="Csv_Writer", table_name="physics")


''' Backend data interactions: query() and find(). Manipulating their outputs to update() the backend'''
query_df = test.query("SELECT * FROM math", collection=True)
test.display("math")
query_df['f'] = 123
query_df["new_col"] = "test"
print(query_df)
test.update(query_df)
test.display("math")

# find_list = test.find(query=2, collection=True)
# for obj in find_list:
#     test.display(table_name=obj.table_name)
# for obj in find_list:
#     address_table = obj
#     address_df = address_table.collection
#     address_df['i'] = 200
#     address_df['j'] = 123.456
#     address_df["new_col"] = "test"
#     address_table.collection = address_df
#     obj = address_table
# test.update(find_list)
# for obj in find_list:
#     test.display(table_name=obj.table_name)


''' Printing table information'''
# test.list()
# test.num_tables()

# test.summary()
# test.summary(table_name='physics')
# test.summary(table_name='physics', num_rows = 3)

# test.display(table_name='physics')
# test.display(table_name='physics', num_rows = 3)
# test.display(table_name='physics', num_rows = 3, display_cols=['n', 'p', 'r'])