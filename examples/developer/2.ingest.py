# examples/developer/2.ingest.py
from dsi.core import Terminal

terminal_ingest = Terminal()

# UNZIP clover3d.zip INSIDE EXAMPLES/CLOVER3D/
terminal_ingest.load_module('plugin', 'Cloverleaf', 'reader', folder_path="../clover3d/clover3d/")

terminal_ingest.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_ingest.artifact_handler(interaction_type='ingest')