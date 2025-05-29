# examples/developer/ingest.py
from dsi.core import Terminal

terminal_ingest = Terminal(debug = 0, backup_db = False, runTable = True)

terminal_ingest.load_module('plugin', 'TOML1', 'reader', filenames=["../data/results.toml", "../data/results1.toml"])

terminal_ingest.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_ingest.artifact_handler(interaction_type='ingest')