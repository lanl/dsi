# examples/ndp/5.list_and_summary.py
"""
Using list() and summary() methods to view cached data.
"""

from dsi.dsi import DSI

def main():
    # Initialize with a search query
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "space", "limit": 10}
    )
    
    # Show table list
    print("\nTable List:")
    dsi.list()
    
    # Get list as collection (returns list of table names)
    print("\nTable List (collection):")
    result = dsi.list(collection=True)
    print(f"Type: {type(result)}")
    print(f"Contents: {result}")
    
    # Show summary of all tables
    print("\nSummary of All Tables:")
    dsi.summary()
    
    # Summary of specific tables
    print("\nDatasets Table Summary:")
    dsi.summary(table_name='datasets')
    
    print("\nResources Table Summary:")
    dsi.summary(table_name='resources')
    
    # Get summary as collection
    print("\nDatasets Summary (collection):")
    result = dsi.summary(table_name='datasets', collection=True)
    print(f"Type: {type(result)}")
    print(result)
    
    dsi.close()

if __name__ == "__main__":
    main()