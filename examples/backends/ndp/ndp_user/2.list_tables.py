# examples/ndp/ndp_user/2.list_tables.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        keywords="climate",
        limit=10
    )
    
    # Print table names and dimensions
    dsi.list()
    
    if verbose:
        # Get table names as a list
        table_names = dsi.list(collection=True)
        print(f"\nFound {len(table_names)} tables:")
        for name in table_names:
            print(f"  - {name}")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)