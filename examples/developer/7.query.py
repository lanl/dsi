# examples/developer/7.query.py
from dsi.core import Terminal

terminal_query = Terminal()

# Run 3.schema.py so schema_data.db is not empty
terminal_query.load_module('backend','Sqlite','back-write', filename='schema_data.db')

data = terminal_query.artifact_handler(interaction_type='query', query = "SELECT * FROM input;")
print(data)

# Use if want to retrieve all data from a table
data = terminal_query.get_table(table_name="input")
print(data)