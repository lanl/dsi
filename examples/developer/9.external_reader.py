# examples/developer/9.external_reader.py
from dsi.core import Terminal

term = Terminal()

# Second input is name of the python file where the Reader/Writer is stored
# Third input is full filepath to the python file 
term.add_external_python_module('plugin', 'text_file_reader', '../test/text_file_reader.py')

print(term.list_available_modules('plugin')) # includes TextFile at end of list

# Second input must be the exact spelling of the class name in the external file
term.load_module('plugin', 'TextFile', 'reader', filenames = "../test/test.txt")

term.load_module('backend','Sqlite','back-write', filename='text_data.db')

term.artifact_handler(interaction_type='ingest')

term.display("people") #name of the table created in the TextFile external reader