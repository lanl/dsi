
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


# ============================================================================
# Example 3: Multi-Organization Comparison
# Compare datasets across multiple organizations
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("org_comparison.db")

# Load datasets from LANL
print("Loading LANL datasets...")
dsi.read({'organization': 'lanl', 'limit': 30}, reader_name="CKAN_Search")

# Load datasets from another organization (if available)
print("\nLoading NOAA datasets...")
dsi.read({'organization': 'noaa', 'keywords': 'weather', 'limit': 30}, 
         reader_name="CKAN_Search")

# Compare total datasets by organization
print("\n=== Organization Comparison ===")
dsi.query("""
    SELECT organization_title, 
           COUNT(*) as total_datasets,
           SUM(num_resources) as total_resources
    FROM ckan_datasets 
    GROUP BY organization_title
""")

# Find most common file formats across all organizations
print("\n=== Popular File Formats ===")
dsi.query("""
    SELECT format, COUNT(*) as count 
    FROM ckan_resources 
    GROUP BY format 
    ORDER BY count DESC 
    LIMIT 10
""")

dsi.close()


# ============================================================================
# Example 4: Advanced Filtering and Analysis
# Search with multiple criteria and perform detailed analysis
# ============================================================================
from dsi.dsi import DSI

dsi = DSI("energy_analysis.db")

# Complex search with multiple filters
search_params = {
    'keywords': 'renewable energy',
    'formats': ['CSV', 'JSON', 'PARQUET'],
    'limit': 50,
    'verbose': False
}

dsi.read(search_params, reader_name="CKAN_Search")

# Find datasets updated in the last year (if you have recent data)
print("\n=== Recently Updated Datasets ===")
dsi.query("""
    SELECT title, metadata_modified, num_resources
    FROM ckan_datasets 
    ORDER BY metadata_modified DESC 
    LIMIT 10
""")

# Get detailed resource information for largest files
print("\n=== Largest Resources ===")
dsi.query("""
    SELECT 
        d.title as dataset_title,
        r.resource_name,
        r.format,
        r.size,
        r.url
    FROM ckan_resources r
    JOIN ckan_datasets d ON r.dataset_id = d.id
    WHERE r.size IS NOT NULL
    ORDER BY r.size DESC
    LIMIT 10
""")

# Find datasets with specific tags
print("\n=== Datasets by Tags ===")
dsi.query("""
    SELECT title, tags, num_resources
    FROM ckan_datasets 
    WHERE tags LIKE '%energy%'
""")

dsi.close()


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
