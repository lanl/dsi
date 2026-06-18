# examples/ndp/ndp_user/format_filter.py
from dsi.dsi import DSI

def main(verbose=False):
    """
    Demonstrates filtering datasets by resource format (CSV, JSON, etc).
    Useful for finding datasets with specific data formats.
    """
    
    # Find datasets with CSV and GeoJSON resources
    dsi = DSI(
        backend_name="NDP",
        keywords="climate",
        formats=["CSV", "GeoJSON"],  # Only datasets with these formats
        limit=15
    )
    
    print("\n=== Climate datasets with CSV or GeoJSON resources ===")
    
    datasets_df = dsi.get_table("datasets", collection=True)
    
    if not datasets_df.empty:
        print(f"\nFound {len(datasets_df)} datasets")
        
        # Get resources table to see formats
        table_list = dsi.list(collection=True)
        resource_tables = [t for t in table_list if t != 'datasets']
        
        if resource_tables:
            print("\n--- Resource Formats by Dataset ---")
            for table_name in resource_tables[:5]:  # Show first 5
                resource_df = dsi.get_table(table_name, collection=True)
                formats = resource_df['format'].unique()
                print(f"\n{table_name}:")
                print(f"  Formats: {', '.join(formats)}")
                print(f"  Total resources: {len(resource_df)}")
        
        if verbose:
            # Show detailed resource breakdown
            print("\n--- Detailed Resource List ---")
            for table_name in resource_tables[:3]:
                resource_df = dsi.get_table(table_name, collection=True)
                dsi.display(
                    table_name,
                    num_rows=5,
                    display_cols=["resource_name", "format", "size"]
                )
    else:
        print("No datasets found with specified formats")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)