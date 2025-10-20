# examples/developer/10.notebook.py
from dsi.core import Terminal

terminal_notebook = Terminal()

#read data
terminal_notebook.load_module('plugin', 'Schema', 'reader', filename="../clover3d/schema.json")
terminal_notebook.load_module('plugin', 'Cloverleaf', 'reader', folder_path="../clover3d/")

#ingest data to Sqlite backend
terminal_notebook.load_module('backend','Sqlite','back-write', filename='jupyter_data.db')
terminal_notebook.artifact_handler(interaction_type='ingest')

#generate Jupyter notebook
terminal_notebook.artifact_handler(interaction_type="notebook")