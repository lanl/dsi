# examples/developer/3.schema.py
from dsi.core import Terminal

terminal = Terminal()

terminal.load_module('plugin', 'Schema', 'reader', filename="../clover3d/schema.json")

terminal.load_module('plugin', 'Cloverleaf', 'reader', folder_path="../clover3d/")

terminal.load_module('backend','Sqlite','back-write', filename='schema_data.db')

terminal.artifact_handler(interaction_type='ingest')

terminal.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')
terminal.transload()