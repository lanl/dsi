# examples/user/3.find.py
from dsi.dsi import DSI

# ASSUMING DATABASE HAS DATA FROM 2.read.py:
find_dsi = DSI()

#dsi.backend(filename, reader)
find_dsi.backend("data.db") 

#dsi.find(value)
find_dsi.find(5.9) # finds the value 5.9 in all cells after backend() loaded

find_dsi.close()