# examples/user/1.baseline.py
from dsi.dsi import DSI

baseline_dsi = DSI()

# Lists available backends, readers, and writers in this dsi installation
baseline_dsi.list_backends()
baseline_dsi.list_readers()
baseline_dsi.list_writers()
