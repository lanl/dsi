from dsi.core import Terminal

terminal_ingest = Terminal(debug=0, backup_db = False, runTable=False)

terminal_ingest.load_module('plugin', 'Schema', 'reader', filename="../data/example_schema.json", target_table_prefix = "student")
terminal_ingest.load_module('plugin', 'YAML1', 'reader', filenames=["../data/student_test1.yml", "../data/student_test2.yml"], target_table_prefix = "student")
terminal_ingest.load_module('plugin', 'TOML1', 'reader', filenames=["../data/results.toml", "../data/results1.toml"], target_table_prefix = "results")

terminal_ingest.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_ingest.artifact_handler(interaction_type='ingest')