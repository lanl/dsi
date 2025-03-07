from dsi.core import Terminal

terminal_notebook = Terminal(debug=0, backup_db = False, runTable=False)

# Assuming there is existing data in data.db
terminal_notebook.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_notebook.artifact_handler(interaction_type="notebook")