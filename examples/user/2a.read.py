# # examples/user/2a.read.py
# from dsi.dsi import DSI

# read_dsi = DSI("data.db") # Target a backend, defaults to SQLite if not defined

# #dsi.read(path, reader)
# read_dsi.read("../clover3d/", 'Cloverleaf') # Read data into memory

# #dsi.display(table_name)
# read_dsi.display("input") # Print the specific table's data from the Cloverleaf data

# read_dsi.close() # cleans DSI memory of all DSI modules - readers/writers/backends



# from dsi.dsi import DSI

# # Database to hold the catalog
# # Create new SQlite database
# read_dsi = DSI("data.db") # Target a backend, defaults to SQLite if not defined

# # Use a REAL resource ID from your CKAN catalog

# # Create columns and sort them in the .db accordingly
# # Generate a relational database

# # Have a schema that correspond to the main details (title, details, etc)
# # Second schema where you have a primary and foreign key where tables for different file types
# # Return from the CKAN query
# read_dsi.read(
#     filenames="e34423ad-36f1-4615-9fb3-153178e237ce",  # ← Replace with real ID
#     reader_name="CKAN",
#     table_name="my_ckan_data"
# )

# # Display the table you just loaded (not "input")
# read_dsi.display("my_ckan_data")  # ← Changed to match table_name above

# read_dsi.close() # cleans DSI memory of all DSI modules - readers/writers/backends

#!/usr/bin/env python3
from dsi.dsi import DSI

dsi = DSI("my_analysis.db")
dsi.read("climate CSV", reader_name="CKAN_Search")
dsi.list()

dsi.query("SELECT * FROM Dict LIMIT 20")

# See all datasets
dsi.query("SELECT title, organization_title FROM ckan_datasets")

# Find all CSV files
dsi.query("SELECT resource_name, url FROM ckan_resources WHERE format='CSV'")

dsi.close()

# Test 2: Reopen the database
dsi2 = DSI("my_analysis.db")
dsi2.list()

# # Count resources by format
# dsi.query("SELECT format, COUNT(*) as count FROM ckan_resources GROUP BY format")

# # Find weather station data
# dsi.query("SELECT * FROM ckan_datasets WHERE title LIKE '%weather%'")

# # Get all resources for a specific dataset
# dsi.query("""
#     SELECT d.title, r.resource_name, r.format, r.url 
#     FROM ckan_datasets d 
#     JOIN ckan_resources r ON d.id = r.dataset_id 
#     WHERE d.title LIKE '%lidar%'
# """)

# # List all available tables
# dsi.list()
