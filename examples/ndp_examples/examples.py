
# ============================================================================
# Example 5: Data Discovery Workflow
# Complete workflow from search to data selection
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("data_discovery.db")

# Step 1: Search for datasets
print("STEP 1: Searching for climate monitoring data...")
dsi.read("climate monitoring CSV", reader_name="CKAN_Search")

# Step 2: Explore dataset metadata
print("\nSTEP 2: Exploring available datasets...")
dsi.query("""
    SELECT 
        title,
        organization_title,
        author,
        num_resources,
        metadata_created
    FROM ckan_datasets
""")

# Step 3: Identify datasets with good documentation
print("\nSTEP 3: Finding well-documented datasets...")
dsi.query("""
    SELECT title, author, maintainer
    FROM ckan_datasets 
    WHERE author IS NOT NULL 
    AND maintainer IS NOT NULL
""")

# Step 4: Find all CSV resources for download planning
print("\nSTEP 4: Listing CSV resources for download...")
dsi.query("""
    SELECT 
        d.title,
        r.resource_name,
        r.format,
        r.url
    FROM ckan_datasets d
    JOIN ckan_resources r ON d.id = r.dataset_id
    WHERE r.format = 'CSV'
    ORDER BY d.title
""")

# Step 5: Export resource URLs for batch download script
print("\nSTEP 5: Datasets loaded into DSI for further analysis")

dsi.close()


# ============================================================================
# Example 6: Persistent Database Usage
# Demonstrate how data persists across sessions
# ============================================================================
from dsi.dsi import DSI

# Session 1: Initial search and load
print("=== SESSION 1: Loading Data ===")
dsi = DSI("persistent_catalog.db")

dsi.read("weather stations CSV", reader_name="CKAN_Search")

print("\nInitial dataset count:")
dsi.query("SELECT COUNT(*) as total FROM ckan_datasets")

dsi.close()

# Session 2: Reopen and continue work (could be days later)
print("\n=== SESSION 2: Reopening Database ===")
dsi2 = DSI("persistent_catalog.db")

print("\nPreviously loaded datasets still available:")
dsi2.list()

print("\nQuerying previously loaded data:")
dsi2.query("SELECT title, organization_title FROM ckan_datasets LIMIT 5")

# Add more data to the same database
print("\n=== Adding More Data ===")
dsi2.read("sensor data JSON", reader_name="CKAN_Search")

print("\nUpdated dataset count:")
dsi2.query("SELECT COUNT(*) as total FROM ckan_datasets")

dsi2.close()


# ============================================================================
# Example 7: Format-Specific Search
# Find datasets with specific file formats only
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("format_search.db")

# Search for Parquet files (efficient columnar format)
print("=== Searching for Parquet Files ===")
dsi.read({'keywords': 'data', 'formats': ['PARQUET'], 'limit': 20}, 
         reader_name="CKAN_Search")

# See what Parquet datasets are available
print("\n=== Available Parquet Datasets ===")
dsi.query("""
    SELECT 
        d.title,
        d.organization_title,
        COUNT(r.resource_id) as parquet_files
    FROM ckan_datasets d
    JOIN ckan_resources r ON d.id = r.dataset_id
    WHERE r.format = 'PARQUET'
    GROUP BY d.id
""")

# Compare file sizes across formats
print("\n=== Average File Size by Format ===")
dsi.query("""
    SELECT 
        format,
        COUNT(*) as file_count,
        AVG(size) as avg_size_bytes
    FROM ckan_resources
    WHERE size IS NOT NULL
    GROUP BY format
    ORDER BY avg_size_bytes DESC
""")

dsi.close()


# ============================================================================
# Example 8: Building a Resource Inventory
# Create a comprehensive inventory of available resources
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("resource_inventory.db")

# Load multiple searches to build comprehensive catalog
print("=== Building Resource Inventory ===")

# Search 1: Climate data
dsi.read({'keywords': 'climate', 'formats': ['CSV', 'JSON'], 'limit': 20}, 
         reader_name="CKAN_Search")

# Search 2: Environmental data
dsi.read({'keywords': 'environment', 'formats': ['CSV', 'JSON'], 'limit': 20}, 
         reader_name="CKAN_Search")

# Generate inventory statistics
print("\n=== Inventory Summary ===")
dsi.query("""
    SELECT 
        COUNT(DISTINCT id) as unique_datasets,
        COUNT(DISTINCT organization_name) as unique_organizations,
        SUM(num_resources) as total_resources
    FROM ckan_datasets
""")

# Create a resource type breakdown
print("\n=== Resource Type Breakdown ===")
dsi.query("""
    SELECT 
        format,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM ckan_resources
    GROUP BY format
    ORDER BY count DESC
""")

# Identify datasets with the most comprehensive resources
print("\n=== Most Comprehensive Datasets ===")
dsi.query("""
    SELECT 
        title,
        organization_title,
        num_resources,
        tags
    FROM ckan_datasets
    ORDER BY num_resources DESC
    LIMIT 10
""")

dsi.close()


# ============================================================================
# Example 9: Search String Variations
# Demonstrate different ways to format search strings
# ============================================================================
from dsi.dsi import DSI

# Example 9a: Simple keyword
dsi = DSI("search_variations.db")
dsi.read("climate", reader_name="CKAN_Search")
dsi.close()

# Example 9b: Keyword + format
dsi = DSI("search_variations.db")
dsi.read("ocean temperature CSV", reader_name="CKAN_Search")
dsi.close()

# Example 9c: Multiple formats
dsi = DSI("search_variations.db")
dsi.read("environmental data CSV JSON XML", reader_name="CKAN_Search")
dsi.close()

# Example 9d: Complex keywords with formats
dsi = DSI("search_variations.db")
dsi.read("renewable energy solar wind CSV", reader_name="CKAN_Search")
dsi.close()

print("All search variations completed successfully!")


# ============================================================================
# Example 10: Finding Related Datasets
# Use SQL to discover related datasets based on tags and keywords
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("related_datasets.db")

# Load initial dataset search
dsi.read("atmospheric science", reader_name="CKAN_Search")

# Find datasets with overlapping tags
print("\n=== Datasets with Related Tags ===")
dsi.query("""
    SELECT 
        title,
        tags,
        organization_title
    FROM ckan_datasets
    WHERE tags LIKE '%atmosphere%' 
       OR tags LIKE '%climate%'
       OR tags LIKE '%weather%'
""")

# Find datasets from the same organizations
print("\n=== Other Datasets from Same Organizations ===")
dsi.query("""
    SELECT 
        organization_title,
        COUNT(*) as dataset_count,
        GROUP_CONCAT(DISTINCT tags) as all_tags
    FROM ckan_datasets
    GROUP BY organization_title
    HAVING dataset_count > 1
""")

# Resources with similar naming patterns
print("\n=== Resources with Similar Names ===")
dsi.query("""
    SELECT 
        resource_name,
        format,
        dataset_name
    FROM ckan_resources
    WHERE resource_name LIKE '%data%'
       OR resource_name LIKE '%measurement%'
    ORDER BY resource_name
""")

dsi.close()
