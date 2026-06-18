# examples/ndp/ndp_user/4.multiple_queries_basic.py
"""
Basic multiple query example - Combining different search terms
"""
from dsi.dsi import DSI

def main(verbose=False):
    # Combine results from multiple searches using params argument
    dsi = DSI(
        backend_name="NDP",
        params=[
            {"keywords": "water quality", "limit": 10},
            {"keywords": "air quality", "limit": 10}
        ]
    )
    
    print("\nLoaded data from multiple queries...")
    
    if verbose:
        print("\n--- Backend Status ---")
        dsi.num_tables()
        
        print("\n--- Available Tables ---")
        dsi.list()
        
        print("\n--- Datasets Table (first 5 rows) ---")
        df = dsi.get_table("datasets", collection=True)
        print(df[["title", "organization", "num_resources"]].head())
    else:
        dsi.list()
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Load NDP data using multiple queries"
    )
    parser.add_argument("--verbose", action="store_true", 
                       help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)