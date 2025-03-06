from dsi.core import Terminal
# IMPORT PLUGIN PYTHON FILE 

term=Terminal(debug=0, backup_db = False, runTable=False)

term.add_external_python_module('plugin', 'plugin_python_file', '/path/to/python_file.py')
term.load_module('plugin', 'MyPlugin', 'reader') # MyPlugin is name of plugin class in the file

term.list_loaded_modules() # includes MyPlugin