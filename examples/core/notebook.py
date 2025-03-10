from dsi.core import Terminal

terminal_notebook = Terminal(debug=0, backup_db = False, runTable=True)

# Run Example 2 so data.db is not empty and data can be outputted to a Python notebook
terminal_notebook.load_module('backend','Sqlite','back-write', filename='data.db')

terminal_notebook.artifact_handler(interaction_type="notebook")