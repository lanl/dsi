from dsi.core import Terminal

term = Terminal(debug=0, backup_db = False, runTable=False)

# Second input is name of plugin class in the other file
# Third input is name of the python file where the Reader/Writer is written
term.add_external_python_module('plugin', 'HACC', 'hacc_reader.py')

# print(term.list_available_modules('plugin')) # includes TextFile at end of list

# term.load_module('plugin', 'HACC', 'reader', filenames = "../data/test.txt")
term.load_module('plugin', 'HACC', 'reader', filename = "./hacc.json", hacc_suite_path = "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/128MPC_RUNS_FLAMINGO_DESIGN_3A/", hacc_run_prefix= 'FSN', target_table_prefix = 'hacc')
# /lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/128MPC_RUNS_FLAMINGO_DESIGN_3A/
# "/lus/eagle/projects/CosDiscover/nfrontiere/SCIDAC_RUNS/256MPC_RUNS_HACC_2PARAM"
print("load module Done!")
term.load_module('backend','Sqlite','back-write', filename="test.db")
term.artifact_handler(interaction_type='ingest')
# print(term.get_current_abstraction())

## Query Test 
# # update dsi abstraction using a query to the sqlite db
table_name = 'hacc__halos'
query_text = "SELECT * FROM hacc__halos WHERE halo_rank > 0 and run_id = 0"

query_data = term.artifact_handler(interaction_type='query', query = query_text, dict_return = True)
term.update_abstraction(table_name, query_data)

#export to csv
term.load_module('plugin', "Csv_Writer", "writer", filename = 'test.csv', table_name = table_name)
term.transload()