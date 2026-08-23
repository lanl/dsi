# examples/maglab/7.write_and_process.py
"""
Process Maglab data into a local database for offline analysis.
"""

from dsi.dsi import DSI

def main():
    # Initialize Maglab backend
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": "8r2b3"}
    )

    print("\n=== Step 1: View Maglab data ===")
    dsi.summary()

    # Process Maglab data to local Sqlite database
    print("\n=== Step 2: Process to local database ===")
    dsi.process(
        backend_name="Sqlite",
        filename="maglab_data.db"
    )
    print("Saved Maglab data to maglab_data.db")

    # Close the Maglab instance
    dsi.close()

    # Load the newly created database
    print("\n=== Step 3: Load local database ===")
    local_dsi = DSI(
        backend_name="Sqlite",
        filename="maglab_data.db"
    )

    print("\n=== Step 4: Query local database ===")
    local_files = local_dsi.query(
        "SELECT name, size_bytes FROM files LIMIT 5",
        collection=True
    )
    print(f"\nQueried {len(local_files)} files from local database:")
    print(local_files[["name", "size_bytes"]])

    local_dsi.close()

    print("\nComplete! Data saved locally for offline analysis.")

if __name__ == "__main__":
    main()
