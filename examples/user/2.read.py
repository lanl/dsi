# examples/user/2.read.py
from dsi.dsi import DSI

read_dsi = DSI("data.db") # Target a backend, defaults to SQLite if not defined

#dsi.read(path, reader)
read_dsi.read("../clover3d/", 'Cloverleaf') # Read data into memory

#dsi.display(table_name)
read_dsi.display("input") # Print the specific table's data from the Cloverleaf data

read_dsi.close() # cleans DSI memory of all DSI modules - readers/writers/backends
