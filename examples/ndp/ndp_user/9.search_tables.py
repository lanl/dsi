# examples/ndp/ndp_user/10.search_tables.py
"""
Using search() to find values across all tables in NDP.
"""

from dsi.dsi import DSI

def main(search_term="NASA", verbose=False):
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "earth science", "limit": 25}
    )
    
    print(f"\n=== Searching for '{search_term}' across all tables ===\n")
    
    # Search returns matches from all tables
    if not verbose:
        dsi.search(search_term, collection=False)
    else:
        # Get results as collection for analysis
        results = dsi.search(search_term, collection=True)
        
        if results:
            print(f"Found {len(results)} matching table(s)\n")
            
            for idx, result_df in enumerate(results, 1):
                print(f"Match #{idx}:")
                
                # Check if it's a table/column match or cell match
                if 'table_name' in result_df.columns:
                    if 'column_name' in result_df.columns:
                        print("  Type: Column name match")
                        for _, row in result_df.iterrows():
                            print(f"  - Table: {row['table_name']}")
                            print(f"    Column: {row['column_name']}")
                    else:
                        print("  Type: Table name match")
                        for _, row in result_df.iterrows():
                            print(f"  - Table: {row['table_name']}")
                else:
                    # Cell data match
                    print("  Type: Cell data match")
                    print(f"  Columns: {list(result_df.columns)}")
                    print(f"  Rows found: {len(result_df)}")
                    
                    if len(result_df) <= 3:
                        print("\n  Data:")
                        print(result_df.to_string(index=False))
                    else:
                        print("\n  Sample data (first 3 rows):")
                        print(result_df.head(3).to_string(index=False))
                
                print()
        else:
            print(f"No matches found for '{search_term}'")
    
    # Additional search examples
    print("\n=== Additional search examples ===\n")
    
    print("1. Search for 'CSV' format:")
    dsi.search("CSV", collection=False)
    
    print("\n2. Search for datasets with many resources:")
    dsi.search(10, collection=False)  # Search for number 10
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", default="NASA", 
                       help="Term to search for")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(search_term=args.search, verbose=args.verbose)