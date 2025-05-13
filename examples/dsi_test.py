from dsi.dsi import DSI

'''This is an example workflow using core.py'''
test = DSI()

''' Printing all valid readers, writers, backends '''
# test.list_readers()
# test.list_writers()
# test.list_backends()

''' Example uses of loading open DSI readers '''
# test.read(filenames="data/example_schema.json", reader_name='Schema')
test.read(filenames=["data/student_test1.yml", "data/student_test2.yml"], reader_name='YAML1')
# test.read(filenames=["data/results.toml", "data/results1.toml"], reader_name='TOML1')
# test.read(filenames="data/yosemite5.csv", reader_name='Csv')
# test.read(filenames="data/wildfiredata.csv", reader_name='Ensemble') # makes a sim table automatically
# test.read(filenames=['data/bueno1.data', 'data/bueno2.data'], reader_name='Bueno')
# test.read(filenames=['wildfire/wildfire_oceans11.yml', 'pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
# test.read(filenames="wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
# test.read(filenames="wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')


''' Example uses of loading open DSI writers. '''
# test.write(filename="er_diagram.png", writer_name="ER_Diagram")
# test.write(filename="physics_plot.png", writer_name="Table_Plot", table_name="physics")
# test.write(filename="physics.csv", writer_name="Csv_Writer", table_name="physics")


''' Example of loading a Sqlite DSI backend, and its data interactions: put (ingest), get (query), inspect (notebook), read (process) '''
test.backend("data.db", backend_name="Sqlite")
# test.backend("data.duckdb", backend_name="DuckDB")

test.ingest()
# test.query("SELECT * FROM physics")
# test.process()
# test.nb() # UPDATING SOON


# ''' Example of printing table information'''
# test.list()
# test.num_tables()

# test.summary()
# test.summary(table_name='student__physics')
# test.summary(table_name='student__physics', num_rows = 3)

# test.display(table_name='student__physics')
# test.display(table_name='student__physics', num_rows = 3)
# test.display(table_name='student__physics', num_rows = 3, display_cols=['n', 'p', 'r'])


# ''' Example uses of the DSI FIND feature: find_table, find_column, find '''
# ## TABLE match                      - prints matching table data
# test.findt(query="people")

# ## COLUMN match                     - prints matching column data
# test.findc(query="a")

# ## RANGE match (range = True)       - prints [min, max] of matching cols
# test.findc(query="avg", range=True)

# ## CELL match                       - prints the cells which match the search term
# test.find(query=5.5)

# ## ROW match (row_return = True)    - prints the rows where cells match the search term
# test.find(query=5.5, row=True)