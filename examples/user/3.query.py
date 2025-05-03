# examples/user/3.query.py
from dsi.dsi import DSI

query_dsi = DSI()

query_dsi.read(filenames="../data/student_test1.yml", reader_name='YAML1')
query_dsi.read(filenames="../data/student_test2.yml", reader_name='YAML1')

query_dsi.backend(filename="data.db", backend_name='Sqlite')

query_dsi.ingest() # need to call backend() before ingest()

query_dsi.query(statement="SELECT * FROM math")

query_dsi.close()

# ---------
# IF DATABASE ALREADY HAS DATA THEN:
query_dsi2 = DSI()

query_dsi2.backend(filename="data.db", backend_name='Sqlite')

query_dsi2.query(statement="SELECT * FROM math") # still need to call backend() before query()

query_dsi2.close()