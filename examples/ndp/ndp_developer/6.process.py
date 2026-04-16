# examples/ndp/ndp_developer/6.process.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()

    # Load NDP backend with initial query
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate", "limit": 10}
    )

    backend = terminal.active_modules["back-read"][0]

    # Process artifacts returns the cached tables (datasets + resource tables)
    tables = backend.process_artifacts()

    if verbose:
        print("\nProcessed artifacts (cached tables):\n")
        for table_name in tables.keys():
            num_rows = len(tables[table_name].get(list(tables[table_name].keys())[0], []))
            num_cols = len(tables[table_name].keys())
            print(f"  [{table_name}]: {num_rows} rows, {num_cols} columns")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP process example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)