# examples/ndp/ndp_developer/3.query.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    
    # Called by ndp __init__
    t.load_module("backend", "NDP", "back-read", webargs={"keywords": "energy", "limit": 10})
    
    # Called by ndp's query_artifact
    result = t.artifact_handler(interaction_type='query', query="`num_resources` > 10", queryargs={"table": "datasets", "dict_return": True})

    # This is a filesystem example
    #data = terminal_query.artifact_handler(interaction_type='query', query = "SELECT * FROM input;")

    if verbose:
        print("Query results (num_resources > 10):" + str(len(result)))

        for table_name, table_data in result.items():
            print(f"\n{table_name}")

            for col, values in table_data.items():
                print(f"{col}: {values}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)