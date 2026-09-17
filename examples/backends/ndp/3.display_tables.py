# examples/ndp/3.display_tables.py
"""
Using display() to view table data with various column selections.
"""

from dsi.dsi import DSI

def main():
    # Initialize with forest datasets
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "forest",
            "tags": ["lidar"],
            "limit": 20
        }
    )
    
    # Display datasets with all columns (default)
    print("\nDatasets Table (All Columns - Default):")
    dsi.display('datasets', num_rows=3)
    
    # Display datasets with minimal selection
    print("\nDatasets (Minimal Selection):")
    dsi.display('datasets', num_rows=3, 
                display_cols=['id', 'name', 'title', 'organization'])
    
    # Display datasets with extended metadata
    print("\nDatasets (Extended Metadata):")
    dsi.display('datasets', num_rows=3,
                display_cols=['id', 'title', 'organization', 'creator', 
                             'created', 'modified', 'num_resources'])
    
    # Display resources with all columns (default)
    print("\nResources Table (All Columns - Default):")
    dsi.display('resources', num_rows=5)
    
    # Display resources with minimal selection
    print("\nResources (Minimal Selection):")
    dsi.display('resources', num_rows=5,
                display_cols=['resource_id', 'resource_name', 'format', 'url'])
    
    # Display resources with extended metadata
    print("\nResources (Extended Metadata):")
    dsi.display('resources', num_rows=5,
                display_cols=['resource_id', 'resource_name', 'format', 
                             'size', 'issue_date', 'dataset_title'])
    
    dsi.close()

if __name__ == "__main__":
    main()