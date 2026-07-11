# examples/ndp/3.load_multiple.py
"""
Multiple NDP queries with automatic deduplication.
Demonstrates ALL available NDP query parameters.
"""

from dsi.dsi import DSI

def main():
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
    
    print("\n=== Query Results ===")
    dsi.list()
    dsi.summary()
    
    # Show unique datasets
    datasets = dsi.get_table("datasets", collection=True)
    print(f"\n=== Retrieved {len(datasets)} unique datasets ===")
    print(datasets[['title', 'organization', 'creator']].to_string())
     
    dsi.close()

if __name__ == "__main__":
    main()