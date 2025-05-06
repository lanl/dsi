# examples/dev_core/external_plugin.py
from dsi.core import Terminal

term = Terminal(debug = 0, backup_db = False, runTable = False)

# Second input is name of plugin class in the other file
# Third input is name of the python file where the Reader/Writer is written
term.add_external_python_module('plugin', 'TextFile', 'text_file_reader.py')

print(term.list_available_modules('plugin')) # includes TextFile at end of list

term.load_module('plugin', 'TextFile', 'reader', filenames = "../data/test.txt")

print(term.get_current_abstraction())
# OrderedDict({'text_file': 
#               OrderedDict({'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'], 
#                            'Age': [25, 30, 22, 28, 35], 
#                            'Location': ['New York', 'Dallas', 'Chicago', 'Miami', 'Boston']})})