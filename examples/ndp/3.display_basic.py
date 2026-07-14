# examples/ndp/3.display_tables.py
"""
Using display() to view table data with column selection, plus collection outputs.
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
    
    # Get list as collection (returns list of table names)
    print("\nTable List (collection):")
    result = dsi.list(collection=True)
    print(f"Type: {type(result)}")
    print(f"Contents: {result}")
    
    # Get summary as collection
    print("\nDatasets Summary (collection):")
    result = dsi.summary(table_name='datasets', collection=True)
    print(f"Type: {type(result)}")
    print(result)
    
    # Display datasets with specific columns
    print("\nDatasets (Selected Columns):")
    dsi.display('datasets', num_rows=3, display_cols=["name", "title", "tags"])
    
    # Extended dataset view with metadata
    print("\nDatasets (Extended View):")
    dsi.display('datasets', num_rows=3,
                display_cols=['id', 'title', 'organization', 'creator', 
                             'created', 'modified', 'num_resources'])
    
    # Display resources with specific columns
    print("\nResources (Selected Columns):")
    dsi.display('resources', num_rows=5, display_cols=["resource_name", "dataset_title", "url"])
    
    # Resource view with metadata
    print("\nResources (With Metadata):")
    dsi.display('resources', num_rows=5,
                display_cols=['resource_name', 'format', 'size', 
                             'issue_date', 'dataset_title'])
    
    dsi.close()

if __name__ == "__main__":
    main()