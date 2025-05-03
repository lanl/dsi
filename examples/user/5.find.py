# examples/user/5.find.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.ingest.py:
find_dsi = DSI()

find_dsi.backend(filename="data.db", backend_name='Sqlite') 

find_dsi.findt() # need to call backend() before findt() to be able to find tables in a backend

find_dsi.findc() # need to call backend() before findc() to be able to find columns in a backend

find_dsi.find() # need to call backend() before find() to be able to find any data from a backend

find_dsi.close()