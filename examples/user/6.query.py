# examples/user/6.query.py
from dsi.dsi import DSI

query_dsi = DSI("data.db") # Assuming data.db has data from 2.read.py:

#dsi.query(sql_statement)
query_dsi.query("SELECT * FROM input")

#dsi.get_table(table_name)
query_dsi.get_table("input") # alternative to query() if want all data

query_dsi.close()