# examples/ndp/ndp_developer/6.process.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate", "limit": 10}
    )
    
    backend = terminal.active_modules["back-read"][0]
    tables = backend.process_artifacts()
    
    if verbose:
        print("\nProcessed tables:")
        for table_name in tables.keys():
            first_col = list(tables[table_name].values())[0]
            print(f"  {table_name}: {len(first_col)} rows")
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)