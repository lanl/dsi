# examples/user/2.ingest.py
from dsi.dsi import DSI

ingest_dsi = DSI()

ingest_dsi.read(filenames="../data/student_test1.yml", reader_name='YAML1')
ingest_dsi.read(filenames="../data/student_test2.yml", reader_name='YAML1')
ingest_dsi.read(filenames="../data/wildfiredata.csv", reader_name='Csv', table_name="wildfire") # table_name can be included if data file has only one table

ingest_dsi.backend(filename="data.db", backend_name='Sqlite')

ingest_dsi.ingest() # need to call backend() before ingest()

ingest_dsi.display(table_name="math") # math is a table name in student_test1.yml

ingest_dsi.display(table_name="wildfire") # wildfire table manually defined above

ingest_dsi.close() # cleans DSI memory of all DSI modules - readers/writers/backends
