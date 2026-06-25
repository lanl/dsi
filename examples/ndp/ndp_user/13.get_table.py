# examples/ndp/ndp_user/3.get_table.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        keywords="ocean",
        limit=15
    )
    
    # Get datasets table as DataFrame
    print("\nGetting datasets table:")
    df = dsi.get_table("datasets", collection=True)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    if verbose:
        print("\nFirst 3 rows:")
        print(df.head(3))
        
        # Get a resource table if available
        table_list = dsi.list(collection=True)
        resource_tables = [t for t in table_list if t != 'datasets']
        
        if resource_tables:
            print(f"\nGetting resource table: {resource_tables[0]}")
            resource_df = dsi.get_table(resource_tables[0], collection=True)
            print(f"Shape: {resource_df.shape}")
            print(resource_df.head(2))
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)