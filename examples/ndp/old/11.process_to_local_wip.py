# examples/ndp/ndp_user/11.process_to_local.py
"""
Using process() to save NDP data to a local Sqlite or DuckDB backend.
"""

from dsi.dsi import DSI
import os

def main(backend_type="Sqlite", verbose=False):
    # Step 1: Load data from NDP
    print("\n=== Step 1: Load data from NDP ===")
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "wildfire", "limit": 15}
    )
    
    print(f"Loaded {dsi.num_datasets()} datasets from NDP")
    if verbose:
        dsi.list()
    
    # Step 2: Determine local filename
    print(f"\n=== Step 2: Save to local {backend_type} backend ===")
    
    if backend_type.lower() == "sqlite":
        local_file = "wildfire_data.db"
    else:  # DuckDB
        local_file = "wildfire_data.duckdb"
    
    # Remove old file if exists
    if os.path.exists(local_file):
        os.remove(local_file)
        print(f"Removed existing {local_file}")
    
    # Process (save) the NDP data locally
    dsi.process(
        backend_name=backend_type,
        filename=local_file
    )
    
    print(f"✓ Successfully saved NDP data to {local_file}")
    
    # IMPORTANT: Close NDP connection
    dsi.close()
    
    # Step 3: Reopen with local backend
    print(f"\n=== Step 3: Open local {backend_type} backend ===")
    
    dsi = DSI(
        backend_name=backend_type,
        filename=local_file
    )
    
    print(f"✓ Now connected to local {backend_type} backend")
    
    # Verify the local backend
    print(f"  - Number of tables: {dsi.num_tables()}")
    
    if verbose:
        print("\nTable names in local backend:")
        tables = dsi.list(collection=True)
        for table in tables:
            print(f"  - {table}")
        
        print("\nSample data from datasets table:")
        dsi.display("datasets", num_rows=5, display_cols=["title", "organization"])
    
    # Step 4: Now you can query locally (much faster!)
    print("\n=== Step 4: Query local backend ===")
    
    # This query runs on your local machine, not NDP
    high_resource_datasets = dsi.query(
        "SELECT title, num_resources FROM datasets WHERE num_resources >= 5",
        collection=True
    )
    
    print(f"Found {len(high_resource_datasets)} datasets with 5+ resources")
    
    if verbose and len(high_resource_datasets) > 0:
        print("\nDatasets with many resources:")
        print(high_resource_datasets.to_string(index=False))
    
    # Step 5: Show the power of local queries
    print("\n=== Step 5: Complex local query ===")
    
    # IMPORTANT: Query must start with SELECT (no leading whitespace)
    complex_query = """SELECT d.title, d.organization, COUNT(r.id) as actual_resource_count, d.num_resources as reported_count
FROM datasets d
LEFT JOIN resources r ON d.id = r.package_id
GROUP BY d.id, d.title, d.organization, d.num_resources
ORDER BY actual_resource_count DESC
LIMIT 5"""
    
    results = dsi.find_relation(complex_query, collection=True)
    
    if verbose and len(results) > 0:
        print("\nTop 5 datasets by resource count:")
        print(results.to_string(index=False))
    else:
        print(f"Found {len(results)} datasets")
    
    # Step 6: Show filtering with find()
    print("\n=== Step 6: Use find() on local backend ===")
    
    # find() also works on local backends
    large_datasets = dsi.find('num_resources > 3', collection=True)
    print(f"Found {len(large_datasets)} datasets with >3 resources using find()")
    
    if verbose and len(large_datasets) > 0:
        print("\nLarge datasets:")
        for idx, row in large_datasets.head(5).iterrows():
            print(f"  - {row['title']} ({row['num_resources']} resources)")
    
    dsi.close()
    
    print(f"\n✓ Local backend saved to: {local_file}")
    print("  You can now reuse this file without querying NDP again!")
    print(f"\nTo reopen later:")
    print(f"  dsi = DSI(backend_name='{backend_type}', filename='{local_file}')")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["Sqlite", "DuckDB"], 
                       default="Sqlite", help="Local backend type")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(backend_type=args.backend, verbose=args.verbose)