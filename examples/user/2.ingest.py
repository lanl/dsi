# examples/user/2.ingest.py
from dsi.dsi import DSI

ingest_dsi = DSI()

#dsi.read(filename, reader)
ingest_dsi.read("../data/student_test1.yml", 'YAML1') # Read data into memory
ingest_dsi.read("../data/student_test2.yml", 'YAML1')

#dsi.backend(filename, backend)
ingest_dsi.backend("data.db") # Target a backend, defaults to SQLite if not defined
ingest_dsi.ingest() # need to call backend() before ingest()

#dsi.display(table_name)
ingest_dsi.display("math") # Print the specific table name in student_test1.yml

ingest_dsi.close() # cleans DSI memory of all DSI modules - readers/writers/backends
