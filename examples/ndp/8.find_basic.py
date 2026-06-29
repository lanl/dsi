# examples/ndp/ndp_user/8.find_basic.py
"""
Basic find operations on NDP datasets.
Demonstrates numeric and string queries with collection parameter.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "fuel", "limit": 20}
    )
    
    if verbose:
        print("\nData Summary:")
        dsi.summary()
    
    # Find datasets with fewer than 5 resources
    print("\nQuery: num_resources < 5")
    dsi.find('num_resources < 5')
    
    # Same query but return DataFrame
    if verbose:
        print("\nSame query with collection=True:")
        results = dsi.find('num_resources < 5', collection=True)
        print(f"Returned {len(results)} rows as DataFrame")
        print(f"Columns: {results.columns.tolist()}")
    
    # Range query
    print("\nQuery: num_resources (3, 7)")
    dsi.find('num_resources (3, 7)')
    
    # String query with partial match
    print("\nQuery: title ~~ 'fire'")
    results = dsi.find("title ~~ 'fire'", collection=True)
    
    if verbose and len(results) > 0:
        print(f"\nFound {len(results)} matches:")
        print(results[['title', 'organization']].head())
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)