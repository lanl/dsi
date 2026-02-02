# # ============================================================================
# # Example 3: Multi-Organization Comparison
# # Compare datasets across multiple organizations
# # ============================================================================
# from dsi.dsi import DSI

# # Create DSI instance with a database
# dsi = DSI("org_comparison.db")

# try:
#     # Load datasets from LANL
#     print("Loading LANL datasets...")
#     dsi.read({'organization': 'lanl', 'limit': 30}, reader_name="CKAN_Search")
#     print("✓ LANL datasets loaded successfully")
    
#     # Load datasets from another organization (if available)
#     print("\nLoading NOAA datasets...")
#     dsi.read({'organization': 'noaa', 'keywords': 'weather', 'limit': 30}, 
#              reader_name="CKAN_Search")
#     print("✓ NOAA datasets loaded successfully")
    
#     # Compare total datasets by organization
#     print("\n" + "="*60)
#     print("=== Organization Comparison ===")
#     print("="*60)
#     org_comparison = dsi.query("""
#         SELECT 
#             organization_title, 
#             COUNT(*) as total_datasets,
#             SUM(num_resources) as total_resources
#         FROM ckan_datasets 
#         GROUP BY organization_title
#         ORDER BY total_datasets DESC
#     """)
#     print(org_comparison)
    
#     # Find most common file formats across all organizations
#     print("\n" + "="*60)
#     print("=== Popular File Formats ===")
#     print("="*60)
#     formats = dsi.query("""
#         SELECT 
#             format, 
#             COUNT(*) as count 
#         FROM ckan_resources 
#         WHERE format IS NOT NULL AND format != ''
#         GROUP BY format 
#         ORDER BY count DESC 
#         LIMIT 10
#     """)
#     print(formats)
    
#     # Additional useful queries
    
#     # Show recently updated datasets from both organizations
#     print("\n" + "="*60)
#     print("=== Recently Updated Datasets ===")
#     print("="*60)
#     recent = dsi.query("""
#         SELECT 
#             name,
#             title,
#             organization_title,
#             metadata_modified,
#             num_resources
#         FROM ckan_datasets 
#         ORDER BY metadata_modified DESC 
#         LIMIT 10
#     """)
#     print(recent)
    
#     # Show dataset statistics
#     print("\n" + "="*60)
#     print("=== Dataset Statistics ===")
#     print("="*60)
#     stats = dsi.query("""
#         SELECT 
#             COUNT(DISTINCT id) as total_datasets,
#             COUNT(DISTINCT organization_title) as total_organizations,
#             SUM(num_resources) as total_resources,
#             AVG(num_resources) as avg_resources_per_dataset
#         FROM ckan_datasets
#     """)
#     print(stats)

# except Exception as e:
#     print(f"\n❌ Error occurred: {type(e).__name__}")
#     print(f"   Message: {str(e)}")
#     print("\nTroubleshooting tips:")
#     print("1. Make sure the DSI library is installed: pip install dsi")
#     print("2. Check if organizations 'lanl' and 'noaa' exist")
#     print("3. Verify your CKAN endpoint is accessible")
    
# finally:
#     # Always close the connection
#     print("\nClosing database connection...")
#     dsi.close()
#     print("✓ Connection closed")
