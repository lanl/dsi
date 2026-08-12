# examples/ndp/4.find_basic.py
"""
Basic find operations on NDP datasets.
Demonstrates practical string-based queries users commonly need.
"""

from dsi.dsi import DSI

def main():
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "fuel", "limit": 20}
    )
    
    print("\nData Summary:")
    dsi.summary()
    
    # Find datasets from a specific organization
    print("\nQuery: organization == 'FASMEE'")
    dsi.find("organization == 'FASMEE'")
    
    # Same query but return DataFrame
    print("\nSame query with collection=True:")
    results = dsi.find("organization == 'FASMEE'", collection=True)
    if len(results) > 0:
        print(f"Returned DataFrame with {len(results)} rows and {len(results.columns)} columns")
    
    # Partial string match - find datasets with 'California' in organization
    print("\nQuery: organization ~~ 'California'")
    results = dsi.find("organization ~~ 'California'", collection=True)
    
    if len(results) > 0:
        print(f"\nFound {len(results)} matches:")
        print(results[['title', 'organization', 'num_resources']].head())
    
    # Find datasets with specific tag
    print("\nQuery: tags ~~ 'lidar'")
    results = dsi.find("tags ~~ 'lidar'", collection=True)
    
    if len(results) > 0:
        print(f"\nFound {len(results)} datasets with 'lidar' tag")
        print(results[['title', 'organization', 'tags']].head())
    
    dsi.close()

if __name__ == "__main__":
    main()