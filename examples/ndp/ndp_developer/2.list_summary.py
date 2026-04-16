# examples/ndp/ndp_developer/2.list_summary.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "energy", "limit": 10}
    )
    
    backend = terminal.active_modules["back-read"][0]

    if verbose:
        print("\n=== List Tables ===")
        backend.list(collection=False)  # Prints table info
        
        print("\n=== Summary of Tables ===")
        summary_df = backend.summary()
        print(summary_df)
        
        # Get collection format
        tables_dict = backend.list(collection=True)
        print("\n=== Table Collections ===")
        print(f"Dataset tables: {tables_dict['datasets']}")
        print(f"Resource tables: {tables_dict['resources'][:3]}...")  # Show first 3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP list and summary example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)