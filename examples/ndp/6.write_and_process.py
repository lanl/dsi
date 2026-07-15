# examples/ndp/6.write_and_process.py
"""
Export NDP data and process to local database for offline analysis.
"""

from dsi.dsi import DSI

def main():
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "water quality", "limit": 20}
    )
    
    print("\n=== Step 1: View NDP data ===")
    dsi.summary()
    
    # Export datasets to CSV
    print("\n=== Step 2: Export datasets to CSV ===")
    dsi.write(
        filename="water_datasets.csv",
        writer_name="Csv",
        table_name="datasets"
    )
    
    # Process NDP data to local Sqlite database
    print("\n=== Step 3: Process to local database ===")
    dsi.process(
        backend_name="Sqlite",
        filename="water_data.db"
    )
    print("Saved NDP data to water_data.db")
    
    # Close the NDP instance
    dsi.close()
    
    # Load the newly created database
    print("\n=== Step 4: Load local database ===")
    local_dsi = DSI(
        backend_name="Sqlite",
        filename="water_data.db"
    )
    
    print("\n=== Step 5: Query local database ===")
    local_datasets = local_dsi.query(
        "SELECT title, organization FROM datasets LIMIT 5",
        collection=True
    )
    print(f"\nQueried {len(local_datasets)} datasets from local database:")
    print(local_datasets[['title', 'organization']])
    
    local_dsi.close()
    
    print("\nComplete! Data saved locally for offline analysis.")

if __name__ == "__main__":
    main()