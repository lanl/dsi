# examples/user/2b.read_csv.py
from dsi.dsi import DSI

csv_dsi = DSI("wildfire_data.db") # Target a backend, defaults to SQLite if not defined

#dsi.read(path/to/csv, "CSV", "optional table name")
csv_dsi.read("../test/wildfiredata.csv", "CSV", "wfdata")

csv_dsi.summary()

#dsi.display(table_name)
csv_dsi.display("wfdata") # Print the wfdata table's data

csv_dsi.close()