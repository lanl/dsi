# examples/ndp/ndp_user/4.load_by_id.py
"""
Direct NDP dataset lookup by ID.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Direct lookup by dataset ID
    # Replace with a real dataset ID from your NDP instance
    dsi = DSI(
        backend_name="NDP",
        params={"id": "eef10919-9caf-4a78-9e08-10403ca50d82"}
    )
    
    if verbose:
        print("\nDataset ID: 'eef10919-9caf-4a78-9e08-10403ca50d82'")
        
        print("\nAvailable tables:")
        dsi.list()
        
        # Get dataset
        datasets_df = dsi.get_table("datasets", collection=True)
        
        if len(datasets_df) > 0:
            print("\nDataset details:")
            dataset = datasets_df.iloc[0]
            print(f"  - ID: {dataset['id']}")
            print(f"  - Title: {dataset['title']}")
            print(f"  - Organization: {dataset['organization']}")
            print(f"  - Resources: {dataset['num_resources']}")
            
            # Show resources
            resources_df = dsi.get_table("resources", collection=True)
            print(f"\nFound {len(resources_df)} resources:")
            
            if len(resources_df) > 0:
                for idx, row in resources_df.iterrows():
                    print(f"  {idx+1}. {row['resource_name']} ({row['format']})")
        else:
            print("\nDataset not found or access denied")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)