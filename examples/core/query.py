from dsi.core import Terminal

terminal_query = Terminal(debug=0, backup_db = False, runTable=True)

# Assuming there is existing data in data.db
terminal_query.load_module('backend','Sqlite','back-write', filename='data.db')

data = terminal_query.artifact_handler(interaction_type='query', query = "SELECT * FROM runTable;")
print(data)