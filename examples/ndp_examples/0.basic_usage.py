#!/usr/bin/env python3
"""
CKAN Search Example - Basic Usage
Demonstrates how to search CKAN catalogs and query metadata using DSI
"""

from dsi.dsi import DSI

# ============================================================================
# Initialize DSI and Search CKAN Catalog
# ============================================================================

# Create/connect to SQLite database to store CKAN metadata
dsi = DSI("my_analysis.db")

# Search CKAN catalog for climate datasets with CSV format
# The search string automatically extracts keywords ("climate") and format ("CSV")
dsi.read("climate CSV", reader_name="CKAN_Search")

# Display all tables loaded into the database
# Should show: ckan_datasets, ckan_resources
dsi.list()

# ============================================================================
# Query the Loaded Metadata
# ============================================================================

# View first 20 rows from any loaded table (generic query)
dsi.query("SELECT * FROM Dict LIMIT 20")

# View dataset information: titles and their organizations
dsi.query("SELECT title, organization_title FROM ckan_datasets")

# Find all CSV resources with their download URLs
dsi.query("SELECT resource_name, url FROM ckan_resources WHERE format='CSV'")

# Close the database connection and clean up DSI memory
dsi.close()

# ============================================================================
# Reopen Database to Demonstrate Persistence
# ============================================================================

# Reconnect to the same database - data persists across sessions
dsi2 = DSI("my_analysis.db")

# Verify previously loaded tables are still available
dsi2.list()

# Close when finished
dsi2.close()
