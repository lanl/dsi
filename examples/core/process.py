from dsi.core import Terminal

terminal_process=Terminal(debug=0, backup_db = False, runTable=True)

terminal_process.load_module('backend','Sqlite','back-read', filename='data/data.db')   
terminal_process.artifact_handler(interaction_type="process")

# runTable is in ER Diagram since flag is set to True
terminal_process.load_module('plugin', 'ER_Diagram', 'writer', filename = 'er_diagram.pdf')

# After loading a plugin WRITER, need to call transload() to execute it
terminal_process.transload()