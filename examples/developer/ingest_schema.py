# examples/developer/ingest_schema.py
from dsi.core import Terminal

terminal_ingest = Terminal(debug = 0, backup_db = False, runTable = True)

# using schema to target a collection of tables which all have a prefix 'student'
terminal_ingest.load_module('plugin', 'Schema', 'reader', filename="../test/example_schema.json", target_table_prefix = "student")

#creates tables from this YAML data which all have a prefix of 'student'
terminal_ingest.load_module('plugin', 'YAML1', 'reader', filenames=["../test/student_test1.yml", "../test/student_test2.yml"], target_table_prefix = "student")

terminal_ingest.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_ingest.artifact_handler(interaction_type='ingest')