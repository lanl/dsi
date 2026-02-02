# examples/user/ckan_search_examples.py

# ============================================================================
# Example 1: Simple Keyword Search
# Search for climate-related datasets and explore results
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("climate_search.db")

# Simple string search - finds datasets containing "climate" with CSV files
dsi.read("wildfire CSV", reader_name="CKAN_Search")

# List all loaded tables
dsi.list()

# Explore what we found
print("\n=== Datasets Found ===")
dsi.query("SELECT title, organization_title, num_resources FROM ckan_datasets")

# See all available CSV resources
print("\n=== CSV Resources ===")
dsi.query("SELECT resource_name, size, url FROM ckan_resources WHERE format='CSV'")

dsi.close()