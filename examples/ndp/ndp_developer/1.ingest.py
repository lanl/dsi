# examples/ndp/ndp_developer/1.ingest.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "data", "limit": 5})

    if verbose:
        print("Datasets loaded:")
        for table_name, table_data in backend.process_artifacts().items():
            print(table_name, list(table_data.keys()))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP ingest example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)