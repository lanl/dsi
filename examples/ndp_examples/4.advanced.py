# # ============================================================================
# # Example 4: Advanced Filtering and Analysis
# # Search with multiple criteria and perform detailed analysis
# # ============================================================================
# from dsi.dsi import DSI

# dsi = DSI("energy_analysis.db")

# # Complex search with multiple filters
# search_params = {
#     'keywords': 'renewable energy',
#     'formats': ['CSV', 'JSON', 'PARQUET'],
#     'limit': 50,
#     'verbose': False
# }

# dsi.read(search_params, reader_name="CKAN_Search")

# # Find datasets updated in the last year (if you have recent data)
# print("\n=== Recently Updated Datasets ===")
# dsi.query("""
#     SELECT title, metadata_modified, num_resources
#     FROM ckan_datasets 
#     ORDER BY metadata_modified DESC 
#     LIMIT 10
# """)

# # Get detailed resource information for largest files
# print("\n=== Largest Resources ===")
# dsi.query("""
#     SELECT 
#         d.title as dataset_title,
#         r.resource_name,
#         r.format,
#         r.size,
#         r.url
#     FROM ckan_resources r
#     JOIN ckan_datasets d ON r.dataset_id = d.id
#     WHERE r.size IS NOT NULL
#     ORDER BY r.size DESC
#     LIMIT 10
# """)

# # Find datasets with specific tags
# print("\n=== Datasets by Tags ===")
# dsi.query("""
#     SELECT title, tags, num_resources
#     FROM ckan_datasets 
#     WHERE tags LIKE '%energy%'
# """)

# dsi.close()

