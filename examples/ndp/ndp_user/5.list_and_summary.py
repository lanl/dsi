# examples/ndp/ndp_user/list_and_summary.py

"""
Using list() and summary() methods.
Shows overview and statistics of cached data.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize with a search query
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "space",
            "limit": 10
        }
    )
    
    # ============================================
    # LIST: Quick tabular overview
    # ============================================
    
    # Print list (shows table names and dimensions)
    if verbose:
        print("\n=== Dataset List (prints) ===")
    dsi.list()
    
    # Get list as collection (returns list of table names)
    if verbose:
        print("\n=== Dataset List (collection) ===")
    result = dsi.list(collection=True)
    if verbose:
        print(f"Type: {type(result)}")
        print(f"Contents: {result}")
    
    # ============================================
    # SUMMARY: Statistical overview
    # ============================================
    
    # Print summary of all tables
    if verbose:
        print("\n=== Summary of All Tables (prints) ===")
    dsi.summary()
    
    # Summary of single table (datasets)
    if verbose:
        print("\n=== Summary of datasets Table ===")
    dsi.summary(table_name='datasets')
    
    # Summary of single table (resources)
    if verbose:
        print("\n=== Summary of resources Table ===")
    dsi.summary(table_name='resources')
    
    # Get summary as collection (returns DataFrame or list of DataFrames)
    if verbose:
        print("\n=== Summary as Collection ===")
    result = dsi.summary(collection=True)
    if verbose:
        print(f"Type: {type(result)}")
        if isinstance(result, list):
            print(f"Length: {len(result)}")
    
    # Get summary of specific table as collection
    if verbose:
        print("\n=== Summary of datasets as DataFrame ===")
    result = dsi.summary(table_name='datasets', collection=True)
    if verbose:
        print(f"Type: {type(result)}")
        print(result.head() if hasattr(result, 'head') else result)
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)