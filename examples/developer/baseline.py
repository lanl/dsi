# examples/developer/baseline.py
from dsi.core import Terminal

base_terminal = Terminal(debug = 0, backup_db = False, runTable = False)

print(base_terminal.list_available_modules('plugin'))
# ['GitInfo', 'Hostname', 'SystemKernel', 'Bueno', 'Csv']

print(base_terminal.list_available_modules('backend'))
# ['Gufi', 'Sqlite', 'Parquet']

print(base_terminal.list_loaded_modules())
# {'writer': [],
#  'reader': [],
#  'back-read': [],
#  'back-write': []}
