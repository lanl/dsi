# examples/ndp/ndp_user/3.load_multiple.py
"""
Multiple NDP queries with automatic deduplication.
Demonstrates ALL available NDP query parameters.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Define multiple search queries with various parameter combinations
    search_params = [
        # Query 1: Basic keyword search
        {
            "keywords": "wildfire california",
            "formats": ["WMS", "WFS", "HTML"],
            "limit": 8
        },
        
        # Query 2: Multiple filters (organization + tags + formats)
        {
            "keywords": "meadows",
            "organization": "California Landscape Metrics",
            "tags": ["sierra nevada"],
            "limit": 2
        },
        
        # Query 3: Author and maintainer filters
        {
            "keyword": "Salton Sea",
            "creator": "Binayak Parida",
            "organization": "UCR Earth and Planetary Sciences",
            "limit": 2
        }
    ]
    
    # Initialize with multiple queries (automatic deduplication by dataset ID)
    dsi = DSI(
        backend_name="NDP",
        params=search_params
    )
    
    if verbose:
        print("\n=== Query Results ===")
        dsi.list()
        dsi.summary()
        
        # Show unique datasets
        datasets = dsi.get_table("datasets", collection=True)
        resources = dsi.get_table("resources", collection=True)
        print(f"\n=== Retrieved {len(datasets)} unique datasets ===")
        print(datasets[['title', 'organization', 'creator']].to_string())
         
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)