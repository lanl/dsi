from dsi.core import Terminal

term = Terminal(debug=0, backup_db = False, runTable=False)

# Second input is name of plugin class in the other file
# Third input is name of the python file where the Reader/Writer is written
term.add_external_python_module('plugin', 'HACC', 'hacc_reader.py')

# print(term.list_available_modules('plugin')) # includes TextFile at end of list

# term.load_module('plugin', 'HACC', 'reader', filenames = "../data/test.txt")
term.load_module('plugin', 'HACC', 'reader', filename = "./hacc.json", hacc_suite_path = "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM", hacc_run_prefix= 'VKIN', target_table_prefix = 'hacc')
print("done")
term.load_module('backend','Sqlite','back-write', filename="test.db")
term.artifact_handler(interaction_type='ingest')
# print(term.get_current_abstraction())