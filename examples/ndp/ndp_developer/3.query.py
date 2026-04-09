# examples/ndp/ndp_developer/3.query.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read", webargs={"keywords": "energy", "limit": 5})
    result = t.artifact_handler(interaction_type='query', query="`num_resources` > 10", queryargs={"table": "datasets", "dict_return": True})

    # This is a filesystem example
    #data = terminal_query.artifact_handler(interaction_type='query', query = "SELECT * FROM input;")

    if verbose:
        print("Query results (num_resources > 10):")
        for table_name, table_data in result.items():
            print(table_name, list(table_data))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)