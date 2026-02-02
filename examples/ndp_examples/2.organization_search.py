
# ============================================================================
# Example 2: Organization-Specific Search
# Find all datasets from Los Alamos National Laboratory
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("lanl_datasets.db")

# Search specifically within LANL organization
search_params = {
    'organization': 'lanl',
    'formats': ['CSV', 'JSON'],  # Only CSV and JSON files
    'limit': 25
}

dsi.read(search_params, reader_name="CKAN_Search")

# Count datasets by license type
print("\n=== License Distribution ===")
dsi.query("""
    SELECT license_title, COUNT(*) as dataset_count 
    FROM ckan_datasets 
    GROUP BY license_title
    ORDER BY dataset_count DESC
""")

# Find datasets with multiple resources
print("\n=== Datasets with Multiple Files ===")
dsi.query("""
    SELECT title, num_resources 
    FROM ckan_datasets 
    WHERE num_resources > 1
    ORDER BY num_resources DESC
""")

dsi.close()