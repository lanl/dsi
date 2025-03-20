# examples/core/schema.py
from dsi.core import Terminal

terminal = Terminal(debug = 0, backup_db = False, runTable = True)

# using schema to target a collection of tables which all have a prefix 'student'
terminal.load_module('plugin', 'Schema', 'reader', filename="../data/example_schema.json", target_table_prefix = "student")

#creates tables from this YAML data which all have a prefix of 'student'
terminal.load_module('plugin', 'YAML1', 'reader', filenames=["../data/student_test1.yml", "../data/student_test2.yml"], target_table_prefix = "student")

terminal.load_module('backend','Sqlite','back-write', filename='data.db')

terminal.artifact_handler(interaction_type='ingest')

terminal.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')
terminal.transload()