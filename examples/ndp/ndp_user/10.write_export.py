# examples/ndp/ndp_user/10.write_export.py
"""
Using write() to export NDP data to CSV and Parquet formats.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "water quality", "limit": 20}
    )
    
    # Step 1: Find datasets of interest
    print("\n=== Step 1: Find datasets ===")
    datasets = dsi.find('num_resources >= 3', collection=True)
    print(f"Found {len(datasets)} datasets with 3+ resources")
    
    if verbose and len(datasets) > 0:
        print("\nDataset titles:")
        for idx, row in datasets.head(3).iterrows():
            print(f"  - {row['title']}")
    
    # Step 2: Export datasets to CSV
    if len(datasets) > 0:
        print("\n=== Step 2: Export to CSV ===")
        dsi.write(
            filename="water_quality_datasets.csv",
            writer_name="Csv",
            table_name="datasets"
        )
        print("✓ Exported datasets table to CSV")
        
        # Step 3: Export to Parquet
        print("\n=== Step 3: Export to Parquet ===")
        dsi.write(
            filename="water_quality_datasets.pq",
            writer_name="Parquet",
            table_name="datasets"
        )
        print("✓ Exported datasets table to Parquet")
        
        # Step 4: Export resources table
        print("\n=== Step 4: Export resources ===")
        resources = dsi.get_table("resources", collection=True)
        
        if len(resources) > 0:
            dsi.write(
                filename="water_quality_resources.csv",
                writer_name="Csv",
                table_name="resources"
            )
            print(f"✓ Exported {len(resources)} resources to CSV")
        
        # Step 5: Export filtered data using collection
        print("\n=== Step 5: Export filtered collection ===")
        csv_resources = resources[resources['format'] == 'CSV']
        
        if len(csv_resources) > 0:
            dsi.write(
                filename="csv_resources_only.csv",
                writer_name="Csv",
                collection=csv_resources
            )
            print(f"✓ Exported {len(csv_resources)} CSV resources")
    
    dsi.close()
    
    print("\n✓ All exports complete! Check your directory for output files.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)