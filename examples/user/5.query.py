# examples/user/5.query.py
from dsi.dsi import DSI

query_dsi = DSI()

#dsi.read(filename, reader)
query_dsi.read("../data/student_test1.yml", 'YAML1')
query_dsi.read("../data/student_test2.yml", 'YAML1')

#dsi.backend(filename, backend)
query_dsi.backend("data.db") # Target a backend, defaults to SQLite if not defined

query_dsi.ingest() # need to call backend() before ingest()

#dsi.query(sql_statement)
query_dsi.query("SELECT * FROM math")

query_dsi.close()

# ---------
# IF DATABASE ALREADY HAS DATA THEN:
query_dsi2 = DSI()

#dsi.backend(filename, backend)
query_dsi2.backend("data.db")

#dsi.query(sql_statement)
query_dsi2.query("SELECT * FROM math") # still need to call backend() before query()

query_dsi2.close()