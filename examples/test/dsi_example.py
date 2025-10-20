from dsi.dsi import DSI

'''This is an example workflow of DSI()'''
test = DSI("data.db")
# test = DSI("data.db", backend_name="DuckDB")

''' Printing all valid backends, readers, writers '''
# test.list_backends()
# test.list_readers()
# test.list_writers()

''' Example uses of loading DSI readers '''
# test.schema(filename="yaml1_schema.json") # must be loaded first
# test.schema(filename="example_schema.json") # must be loaded first

test.read(filenames=["student_test1.yml", "student_test2.yml"], reader_name='YAML1')
# test.read(filenames=["results.toml", "results1.toml"], reader_name='TOML1')
# test.read(filenames="yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
# test.read(filenames="wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
# test.read(filenames=['bueno1.data', 'bueno2.data'], reader_name='Bueno')
# test.read(filenames="../clover3d/", reader_name='Cloverleaf')
# test.read(filenames="test.txt", reader_name="text_file_reader.py")
# test.read(filenames="wildfiredata.pq", reader_name="Parquet", table_name="Wildfire_parquet")

# test.read(filenames=['../wildfire/wildfire_oceans11.yml', '../pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
# test.read(filenames="../wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
# test.read(filenames="../wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
# test.read(filenames="../wildfire/wildfire_google.yml", reader_name='GoogleDatacard')


''' Example uses of loading DSI writers '''
# test.write(filename="er_diagram.png", writer_name="ER_Diagram")
# test.write(filename="physics_plot.png", writer_name="Table_Plot", table_name="physics")
# test.write(filename="physics.csv", writer_name="Csv_Writer", table_name="physics")
# test.write(filename="wildfire_test.pq", writer_name="Parquet_Writer", table_name="Wildfire_parquet")


''' Backend data interactions: query()/get_table() and find(). Manipulating their outputs to update() the backend '''
test.query("SELECT * from math;")                                             # print output
# test.get_table("math")                                                        # print output
# query_df = test.query("SELECT * FROM math", collection=True, update = True)   # return output
# query_df = test.get_table("math", collection=True, update = True)             # return output
# test.display("math")
# query_df['f'] = 123
# query_df["new_col"] = "test"
# print(query_df)
# last_row = query_df.iloc[-1]
# new_row = {col: (val + 1 if pd.api.types.is_numeric_dtype(query_df[col]) else f"{val} 111") for col, val in last_row.items()}
# query_df.loc[len(query_df)] = new_row
# test.update(query_df)
# test.display("math")

# test.search(2)
test.find(query="a<2")                                                # print output
# find_df = test.find(query="a < 2", collection=True, update = True)    # return output
# test.display(table_name=find_df["dsi_table_name"][0])

# find_df['i'] = list(range(3000, 3000 + len(find_df)))
# find_df['b'] = list(range(3000, 3000 + len(find_df)))
# find_df["new_col"] = "test1"
# print(find_df)

# test.update(find_df, backup=False)
# test.display(table_name=find_df["dsi_table_name"][0])


''' Printing table information '''
# table_list = test.list(True)
# print(table_list)
# test.num_tables()

# test.summary()
# all_dfs = test.summary(collection= True)
# print(all_dfs)
# test.summary(table_name='physics')
# summary_df = test.summary(table_name='physics', collection= True)
# print(summary_df)

# test.display(table_name='physics')
# test.display(table_name='physics', num_rows = 3)
# test.display(table_name='physics', num_rows = 3, display_cols=['n', 'p', 'r'])