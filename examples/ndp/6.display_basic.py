# examples/ndp/ndp_user/6.display_basic.py
"""
Using display() to view table data.
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
    
    # Display datasets table (all columns by default)
    print("\nDatasets Table (All Columns):")
    dsi.display('datasets')
    
    # Display with specific columns only
    print("\nDatasets (Selected Columns):")
    dsi.display('datasets', num_rows=3, display_cols=["name", "title", "tags"])
    
    # Display resources table (all columns by default)
    print("\nResources Table (All Columns):")
    dsi.display('resources')
    
    # Display resources with specific columns only
    print("\nResources (Selected Columns):")
    dsi.display('resources', num_rows=5, display_cols=["resource_name", "dataset_title", "url"])
    
    dsi.close()

if __name__ == "__main__":
    main()