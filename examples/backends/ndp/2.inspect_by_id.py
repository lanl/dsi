# examples/ndp/2.inspect_by_id.py
"""
Direct dataset lookup by ID and inspecting it using list(), summary(), and display().
"""

from dsi.dsi import DSI

def main():
    # Direct lookup by dataset ID
    dataset_id = "eef10919-9caf-4a78-9e08-10403ca50d82"
    
    dsi = DSI(
        backend_name="NDP",
        params={"id": dataset_id}
    )
    
    print(f"\nDataset ID: '{dataset_id}'")
    
    print("\nAvailable tables:")
    dsi.list()
    
    print("\nDatasets Table Summary:")
    dsi.summary(table_name='datasets')
    
    print("\nDataset Details (Selected Columns):")
    dsi.display('datasets', display_cols=['id', 'title', 'organization', 'creator', 'num_resources'])
    
    print("\nResources Table Summary:")
    dsi.summary(table_name='resources')
    
    print("\nResources (Selected Columns):")
    dsi.display('resources', num_rows=5, display_cols=['resource_name', 'format', 'size', 'url'])
    dsi.close()

if __name__ == "__main__":
    main()