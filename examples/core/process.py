from dsi.core import Terminal

terminal_process = Terminal(debug=0, backup_db = False, runTable=True)

# Run Example 2 so data.db is not empty and data can be processed back into the abstraction
terminal_process.load_module('backend','Sqlite','back-read', filename='data.db')   
terminal_process.artifact_handler(interaction_type="process")

# runTable is in ER Diagram since flag was set to True in Example 2 when creating data.db
terminal_process.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.png')

# After loading a plugin WRITER, need to call transload() to execute it
terminal_process.transload()