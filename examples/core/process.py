from dsi.core import Terminal

terminal_process=Terminal(debug=0, backup_db = False, runTable=False)

terminal_process.load_module('backend','Sqlite','back-read', filename='data/data.db')   
terminal_process.artifact_handler(interaction_type="process")

# After loading a plugin WRITER, need to call transload() to execute it
terminal_process.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')
terminal_process.transload()