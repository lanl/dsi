# examples/ndp/3.query.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "energy", "limit": 5})

    result = backend.query_in_memory(
        "`num_resources` > 10",
        {"table": "datasets", "dict_return": True}
    )

    if verbose:
        print("Query results (num_resources > 10):")
        for table_name, table_data in result.items():
            print(table_name, list(table_data))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)