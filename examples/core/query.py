from dsi.core import Terminal

terminal_query=Terminal(debug=0, backup_db = False, runTable=False)

terminal_query.load_module('backend','Sqlite','back-write', filename='data/data.db')

data = terminal_query.artifact_handler(interaction_type='query', query = "SELECT * FROM runTable;")
print(data)