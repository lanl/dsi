# examples/ndp/ndp_developer/5.find.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    # Load NDP backend with initial data
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate", "limit": 10}
    )
    
    backend = terminal.active_modules["back-read"][0]

    # Find methods work on the cached in-memory data
    tables_found = backend.find_table("datasets")
    columns_found = backend.find_column("title")
    cells_found = backend.find_cell("Canada")

    if verbose:
        print("\n=== Find Results ===")
        print(f"\nTables matching 'datasets': {len(tables_found)}")
        for v in tables_found:
            print(f"  - {v.t_name} (columns: {', '.join(v.c_name[:3])}...)")
        
        print(f"\nColumns matching 'title': {len(columns_found)}")
        for v in columns_found[:5]:  # Show first 5
            print(f"  - Table: {v.t_name}, Column: {v.c_name[0]}")
        
        print(f"\nCells matching 'Canada': {len(cells_found)}")
        for v in cells_found[:5]:  # Show first 5
            print(f"  - Table: {v.t_name}, Column: {v.c_name[0]}, Row: {v.row_num}, Value: {v.value}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP find example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)