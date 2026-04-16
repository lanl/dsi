# examples/ndp/ndp_developer/3.display.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "water", "limit": 8}
    )
    
    backend = terminal.active_modules["back-read"][0]

    if verbose:
        print("\n=== Display datasets table ===")
        backend.display("datasets", num_rows=5)
        
        # Display specific columns
        print("\n=== Display datasets (specific columns) ===")
        backend.display("datasets", num_rows=3, display_cols=["title", "organization", "num_resources"])
        
        # Display a resource table if available
        if backend._resource_tables:
            resource_table = backend._resource_tables[0]
            print(f"\n=== Display {resource_table} ===")
            backend.display(resource_table, num_rows=5, display_cols=["resource_name", "format", "url"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP display example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)