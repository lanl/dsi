# examples/user/5.query.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.read.py:
query_dsi = DSI()

#dsi.backend(filename, backend)
query_dsi.backend("data.db")

#dsi.query(sql_statement)
query_dsi.query("SELECT * FROM math") # still need to call backend() before query()

query_dsi.close()