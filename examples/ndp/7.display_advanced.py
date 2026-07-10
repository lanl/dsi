# examples/ndp/ndp_user/7.display_advanced.py
"""
Advanced display options with custom column selection.
"""

from dsi.dsi import DSI

def main():
    # Initialize with earth datasets
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "earth element", "limit": 10}
    )
    
    # Display all columns (default behavior)
    print("\nDatasets (All Columns - Default):")
    dsi.display('datasets', num_rows=3)
    
    # Minimal dataset view
    print("\nDatasets (Minimal - Custom Selection):")
    dsi.display('datasets', display_cols=['id', 'name', 'title', 'organization'])
    
    # Extended dataset view with metadata
    print("\nDatasets (Extended - Custom Selection):")
    dsi.display('datasets', display_cols=['id', 'title', 'organization', 'creator', 
                                          'created', 'modified', 'num_resources'])
    
    # Display all resource columns (default behavior)
    print("\nResources (All Columns - Default):")
    dsi.display('resources', num_rows=3)
    
    # Minimal resource view
    print("\nResources (Minimal - Custom Selection):")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 'url'])
    
    # Resource view with metadata
    print("\nResources (With Metadata - Custom Selection):")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 
                                          'size', 'issue_date', 'dataset_title'], 
                              num_rows=5)
    
    dsi.close()

if __name__ == "__main__":
    main()