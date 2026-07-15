# examples/ndp/ndp_user/4.search.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        keywords="water quality",
        limit=10
    )
    
    # Search for CSV format across all tables
    print("\nSearching for 'CSV':")
    dsi.search("CSV")
    
    if verbose:
        # Get search results as list of DataFrames
        results = dsi.search("water", collection=True)
        if results:
            print(f"\nFound matches in {len(results)} location(s)")
            for i, df in enumerate(results):
                print(f"\nResult {i+1}:")
                print(df.head())
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)