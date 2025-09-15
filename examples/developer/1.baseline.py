# examples/developer/1.baseline.py
from dsi.core import Terminal

#optional parameters
#   debug = 0, 1, 2 (creates log file to analyze runtime)
#   backup_db = True/False (creates backup database prior to ingesting data)
#   runTable = True/False (creates a runTable to organize rows in all tables by ingest number)
base_terminal = Terminal(debug = 0, backup_db = False, runTable = False)

print(base_terminal.list_available_modules('plugin'))
# ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

print(base_terminal.list_available_modules('backend'))
# ['Gufi', 'Sqlite', 'DuckDB', 'HPSS']

print(base_terminal.list_loaded_modules())
# {'writer': [],
#  'reader': [],
#  'back-read': [],
#  'back-write': []}
