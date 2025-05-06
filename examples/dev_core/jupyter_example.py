# examples/dev_core/jupyter_example.py
from dsi.core import Terminal

term = Terminal(debug = 0, backup_db = False, runTable = False) #do not need a runTable here

#read data
term.load_module('plugin', 'Schema', 'reader', filename="../data/example_schema.json")
term.load_module('plugin', 'YAML1', 'reader', filenames=["../data/student_test1.yml"])

#ingest data to Sqlite backend
term.load_module('backend','Sqlite','back-write', filename='data.db')
term.artifact_handler(interaction_type='ingest')

#generate Jupyter notebook
term.artifact_handler(interaction_type="notebook")