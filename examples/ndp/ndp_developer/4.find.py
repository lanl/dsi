# examples/ndp/ndp_developer/4.find.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "climate", "limit": 5})

    tables_found = backend.find_table("datasets")
    columns_found = backend.find_column("title")
    cells_found = backend.find_cell("Canada")

    if verbose:
        print("Tables matching 'datasets':", [v.t_name for v in tables_found])
        print("Columns matching 'title':", [v.c_name for v in columns_found])
        print("Cells matching 'Canada':", [(v.t_name, v.c_name, v.value) for v in cells_found])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP find example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)