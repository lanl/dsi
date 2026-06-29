# examples/ndp/ndp_user/5.list_and_summary.py
"""
Using list() and summary() methods to view cached data.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize with a search query
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "space", "limit": 10}
    )
    
    # Show table list
    if verbose:
        print("\nTable List:")
    dsi.list()
    
    # Get list as collection (returns list of table names)
    if verbose:
        print("\nTable List (collection):")
        result = dsi.list(collection=True)
        print(f"Type: {type(result)}")
        print(f"Contents: {result}")
    
    # Show summary of all tables
    if verbose:
        print("\nSummary of All Tables:")
    dsi.summary()
    
    # Summary of specific tables
    if verbose:
        print("\nDatasets Table Summary:")
        dsi.summary(table_name='datasets')
        
        print("\nResources Table Summary:")
        dsi.summary(table_name='resources')
    
    # Get summary as collection
    if verbose:
        print("\nDatasets Summary (collection):")
        result = dsi.summary(table_name='datasets', collection=True)
        print(f"Type: {type(result)}")
        print(result)
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)