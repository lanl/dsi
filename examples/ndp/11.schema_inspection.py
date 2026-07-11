# examples/ndp/11.schema_inspection.py
"""
Using schema() to inspect table structure and column types.
"""

from dsi.dsi import DSI

def main():
    # Initialize NDP backend
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "climate forest", "limit": 15}
    )
    
    # View full database schema
    print("\n=== Full Database Schema ===")
    full_schema = dsi.schema()
    print(full_schema)
    
    # View specific table schemas
    print("\n=== Datasets Table Schema ===")
    datasets_schema = dsi.schema("datasets")
    print(datasets_schema)
    
    print("\n=== Resources Table Schema ===")
    resources_schema = dsi.schema("resources")
    print(resources_schema)
    
    # Show how schema informs queries
    print("\n=== Using Schema for Queries ===")
    print("Schema shows 'num_resources' is INTEGER:")
    results = dsi.find('num_resources >= 5', collection=True)
    print(f"  ✓ Found {len(results)} datasets with 5+ resources")
    
    print("\nSchema shows 'title' is TEXT:")
    results = dsi.find("title ~~ 'forest'", collection=True)
    print(f"  ✓ Found {len(results)} datasets with 'forest' in title")
    
    dsi.close()

if __name__ == "__main__":
    main()