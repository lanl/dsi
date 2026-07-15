# examples/ndp/5.search_tables.py
"""
Using search() to find values across all tables in NDP.
"""

from dsi.dsi import DSI

def main():
    search_term = "NASA"
    
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "earth science", "limit": 25}
    )
    
    print(f"\n=== Searching for '{search_term}' ===\n")
    
    # Get results as collection for analysis
    results = dsi.search(search_term, collection=True)

    print(f"Found matches in {len(results)} table(s)\n")
    
    for result_df in results:
        # Detect which table this is from
        if 'title' in result_df.columns and 'organization' in result_df.columns:
            print("Datasets Table:")
            # Show key dataset columns
            essential_cols = ['title', 'organization', 'num_resources']
            available_cols = [col for col in essential_cols if col in result_df.columns]
            display_df = result_df[available_cols].drop_duplicates()
            print(display_df.to_string(index=False))
            
        else:
            print("Resources Table:")
            # Show key resource columns
            essential_cols = ['resource_name', 'format', 'dataset_title']
            available_cols = [col for col in essential_cols if col in result_df.columns]
            display_df = result_df[available_cols].drop_duplicates()
            print(display_df.to_string(index=False))
        
        print()
    
    dsi.close()

if __name__ == "__main__":
    main()