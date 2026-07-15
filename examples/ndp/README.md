# NDP Backend for DSI

A read-only backend for accessing NDP catalogs via the CKAN API. Metadata is retrieved and exposed as DSI-compatible tables: datasets and resources.

The National Data Platform (NDP) is a comprehensive data catalog providing open access to datasets from government agencies, research institutions, and other organizations. NDP uses CKAN (Comprehensive Knowledge Archive Network) software to organize and publish diverse datasets with rich metadata. This backend retrieves metadata through the CKAN API, normalizes the returned information, and exposes it as DSI-compatible tables.

Useful NDP resources:

- [National Data Platform](https://nationaldataplatform.org/): Main catalog for searching, browsing, and accessing datasets from various organizations.
- [NDP Catalog Search](https://nationaldataplatform.org/catalog): Browse and search the complete dataset catalog.
- [CKAN API Documentation](https://docs.ckan.org/en/2.9/api/index.html): Official API reference for querying and retrieving dataset metadata.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote NDP data.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses the CKAN API with the following endpoints:

- Base URL: `https://nationaldataplatform.org/catalog`
- API version: 3
- Main endpoint: `/api/3/action/package_search`
- Dataset lookup: `/api/3/action/package_show`

Query parameters are automatically formatted and validated by the backend.

</details>

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={"keywords": "climate", "limit": 10}
)
```

### List Available Tables

```python
dsi.list()
```

### Access a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
print(datasets_df)
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

The backend supports flexible querying through a unified `params` interface.

### Keyword Search

Search across dataset titles, descriptions, and tags:

```python
dsi = DSI(
    backend_name="NDP",
    params={"keywords": "water quality", "limit": 20}
)
```

### Organization Search

Filter datasets by publishing organization:

```python
dsi = DSI(
    backend_name="NDP",
    params={"organization": "BurnPro3D", "limit": 15}
)
```

> **Note:** Organization names with spaces are automatically handled. Use the exact organization name as it appears in NDP.

### Creator Search

Filter datasets by creator/author name:

```python
dsi = DSI(
    backend_name="NDP",
    params={"creator": "Ritambhara Dubey", "limit": 5}
)
```

### Tag Search

Filter datasets by one or more tags (uses AND logic - datasets must have ALL specified tags):

```python
dsi = DSI(
    backend_name="NDP",
    params={"tags": ["USFS", "lidar"], "limit": 10}
)
```

### Groups Search

Filter datasets by collection/group names:

```python
dsi = DSI(
    backend_name="NDP",
    params={"groups": "data_hub_cc_wstc", "limit": 10}
)
```


### Format Search

Filter datasets by resource file formats:

```python
dsi = DSI(
    backend_name="NDP",
    params={"formats": ["CSV", "GeoJSON"], "limit": 10}
)
```

Common formats include: `CSV`, `JSON`, `GeoJSON`, `XML`, `PDF`, `ZIP`, `SHP` (shapefile)

### License Search

Filter datasets by license type:

```python
dsi = DSI(
    backend_name="NDP",
    params={"license": "CC0-1.0", "limit": 10}
)
```

### Combined Parameters

Combine multiple search criteria (uses AND logic - datasets must match ALL specified criteria):

```python
dsi = DSI(
    backend_name="NDP",
    params={
        "keywords": "climate",
        "organization": "California Landscape Metrics",
        "tags": ["climate refugia"],
        "formats": ["GeoTiff"],
        "limit": 25
    }
)
```

### Multiple Independent Queries

Run multiple queries and combine results (uses OR logic - returns datasets matching ANY query, deduplicates by dataset ID):

```python
dsi = DSI(
    backend_name="NDP",
    params=[
        # Query 1: Basic keyword search with format filters
        {
            "keywords": "wildfire california",
            "formats": ["WMS", "WFS", "HTML"],
            "limit": 8
        },
        
        # Query 2: Multiple filters (organization + tags)
        {
            "keywords": "meadows",
            "organization": "California Landscape Metrics",
            "tags": ["sierra nevada"],
            "limit": 2
        },
        
        # Query 3: Author and organization filters
        {
            "keywords": "Salton Sea",  # Fixed: was "keyword" (singular)
            "creator": "Binayak Parida",
            "organization": "UCR Earth and Planetary Sciences",
            "limit": 2
        }
    ]
)
```

---

## Supported Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `keywords` | str | Full-text search across titles, descriptions, tags |
| `organization` | str | Filter by publishing organization name |
| `creator` | str | Filter by dataset creator/author name |
| `tags` | list[str] | Filter by one or more tags (AND logic) |
| `groups` | list[str] | Filter by collection/group names |
| `formats` | list[str] | Filter by resource file formats (OR logic) |
| `license` | str | Filter by license name |
| `limit` | int | Maximum number of datasets to retrieve per query (default: 100) |

---

## Tables

The backend returns two DSI tables:

1. **datasets** - Dataset metadata (one row per dataset)
2. **resources** - Resource files (combined from all datasets)

---

### datasets Table:

The `datasets` table contains one row per NDP dataset.

Table fields include:

| Column | Description |
|--------|-------------|
| id | Unique dataset identifier (CKAN package ID) |
| name | Machine-readable dataset name |
| title | Human-readable dataset title |
| notes | Dataset description/abstract |
| organization | Publishing organization name |
| creator | Dataset creator/author name |
| creator_email | Creator email address |
| groups | Comma-separated list of groups/collections |
| license | License title |
| created | Dataset creation date |
| modified | Last modification date |
| tags | Comma-separated list of tags |
| num_resources | Number of associated resource files |
| raw_dataset | Full CKAN API response for the dataset |

**Example:**

```python
datasets_df = dsi.get_table("datasets", collection=True)
print(datasets_df[["title", "organization", "num_resources"]])
```

---

### resources Table:

The `resources` table contains downloadable file-based or file-specific metadata associated with each dataset. Each row corresponds to a single downloadable resource. The backend does not download files automatically during metadata retrieval; it follows a search-then-retrieval workflow.

Table fields are a subset of the following properties:

| Column | Description |
|--------|-------------|
| resource_id | Unique resource identifier |
| resource_name | Resource file name |
| issue_date | Resource publication date |
| format | File format (CSV, JSON, etc.) |
| size | File size in bytes |
| url | Direct download URL |
| dataset_id | Parent dataset ID |
| dataset_title | Parent dataset title |
| raw_resource | Full CKAN API response for the resource |

**Example:**

```python
resources_df = dsi.get_table("resources", collection=True)
print(resources_df[["resource_name", "format", "url", "dataset_title"]])
```

#### Relationship Between Tables

```text
datasets.id
    |
    |
    V
resources.dataset_id
```

The `id` field in the `datasets` table matches the `dataset_id` field in the `resources` table.

This creates a one-to-many relationship: each dataset can have multiple resources (different formats, APIs, files, etc.), but each resource belongs to exactly one dataset.

**Example:**

```text
datasets
---------
Biomass Power Plants
California Electric Power Plants

resources
---------
Biomass Power Plants -> [WMS] Biomass Power Plants
Biomass Power Plants -> [WFS] Biomass Power Plants
Biomass Power Plants -> [DATA] Biomass Power Plants
Biomass Power Plants -> California Landscape Metrics Website
California Electric Power Plants -> ArcGIS Hub Dataset
California Electric Power Plants -> Esri Rest API
California Electric Power Plants -> GeoJSON
California Electric Power Plants -> CSV
California Electric Power Plants -> KML
California Electric Power Plants -> Shapefile
```

---

## Metadata

### Curated Metadata

Frequently used metadata fields are extracted into table columns for easy access:

- Filtering
- Searching  
- Summarization
- Reporting

These appear directly in `datasets` and `resources` table columns.

### Full Metadata

The complete CKAN API response is preserved in:

```python
raw_dataset   # Full dataset API response
raw_resource  # Full resource API response
```

**Example:**

```python
datasets_df = dsi.get_table("datasets", collection=True)
full_metadata = datasets_df.iloc[0]["raw_dataset"]
```

This ensures:
- No metadata loss
- Future compatibility
- Access to all original API fields
- Support for advanced workflows

---

## Common DSI Operations

### List Tables

```python
dsi.list()
```

**Output:**
```
Table: datasets
  - num of columns: 13
  - num of rows: 10

Table: resources
  - num of columns: 9
  - num of rows: 42
```

### View Backend Summary

```python
dsi.summary()  # Shows SQL-style types: INTEGER, TEXT, OBJECT
```

**Output Displays:**
- Column names and data types (INTEGER, TEXT, OBJECT)
- Unique value counts
- Min/max values for numeric columns
- Statistical summaries

### View Table Schema

```python
dsi.schema("datasets")  # Shows SQL CREATE TABLE format
```

**Output:**
```sql
CREATE TABLE datasets (
    id TEXT,
    name TEXT,
    title TEXT,
    notes TEXT,
    organization TEXT,
    creator TEXT,
    ...
)
```

### Retrieve a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
resources_df = dsi.get_table("resources", collection=True)
```

### Search Loaded Metadata

```python
dsi.search("CSV")  # Prints ALL cells from rows containing "CSV"
```

**Note:** `search()` displays complete matching rows (all columns), not just matched cells. Searches across:
- Table names
- Column names  
- Cell values

### Filter Data

```python
# Find rows where condition is met
results = dsi.find("num_resources > 5")
```

Supports operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~` (contains)

### Display Table Preview

```python
dsi.display("datasets", num_rows=5)
dsi.display("resources", num_rows=10)
```

**Note:** `display()` shows ALL columns by default. Use `display_cols` parameter to limit columns:

```python
# Show only specific columns
dsi.display("datasets", num_rows=5, display_cols=["title", "organization", "creator"])
```

---

## Example Scripts

The following example scripts demonstrate common workflows with the NDP backend.

### 1. load_basic.py

Initialize the NDP backend with a simple keyword search and view available tables.

- Basic query with `keywords` parameter
- Use `list()` to see available tables
- Use `summary()` to view table statistics
- Introduction to NDP backend structure

### 2. load_advanced.py

Advanced query with multiple filter parameters combined.

- Use multiple parameters: `keywords`, `organization`, `groups`, `tags`, `formats`
- Filter datasets with complex criteria
- Display resources table with all columns
- View filtered results

### 3. load_multiple.py

Run multiple independent queries and combine results.

- Execute multiple queries in one DSI initialization
- Demonstrates ALL available NDP query parameters
- Automatic deduplication by dataset ID
- View combined results from different search criteria

### 4. load_by_id.py

Load a specific dataset using its ID or name.

- Direct dataset lookup using `id` parameter
- Access specific dataset without searching
- View dataset details and associated resources
- Useful when you know the exact dataset you need

### 5. list_and_summary.py

Explore table structure and get statistical summaries.

- Use `dsi.list()` to see available tables
- Get list as collection (returns list of table names)
- Use `dsi.summary()` to get column statistics with SQL-style types
- View data types (INTEGER, TEXT, OBJECT), unique values, min/max, averages

### 6. display_basic.py

Preview table data with basic display options.

- Use `dsi.display()` to see table contents (shows ALL columns by default)
- Control number of rows displayed
- Use `display_cols` parameter to select specific columns
- View both datasets and resources tables

### 7. display_advanced.py

Advanced display options for customized table views.

- Show all columns with default behavior
- Specify exact columns to display with custom selection
- Create minimal, extended, and metadata-rich views
- Control output formatting for specific needs

### 8. find_basic.py

Use `find()` to filter rows based on conditions.

- Find rows where a condition is true
- Use operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~`
- Filter numeric and text columns
- Return results as DataFrame with `collection=True`
- Range queries and partial string matching

### 9. search_tables.py

Search for specific values across all tables and use search with query operations.

- Use `dsi.search()` to find values anywhere (prints ALL cells of matching rows)
- Search in table names, column names, and cell values
- **Demonstrates search functionality with `query()` operations**
- Get results as collection for analysis
- View results organized by match type

### 10. write_export.py

Export NDP data to external formats.

- Use `find()` to filter datasets of interest
- Write datasets table to CSV
- Export resources to Parquet
- Export filtered data collections
- Save specific subsets (e.g., CSV resources only)

### 11. schema_inspection.py

Use schema() to inspect table structure and column types.

- View full database schema in SQL CREATE TABLE format
- View specific table schemas (datasets, resources)
- Shows SQL-style column types (INTEGER, TEXT, OBJECT)
- Demonstrates how schema informs query writing
- Examples of type-appropriate queries based on schema

### 12. count_tables_datasets.py

Demonstrates `num_tables()` vs `num_datasets()` in NDP backend.

- Use `dsi.num_tables()` to count logical tables (datasets + resources)
- Use `dsi.num_datasets()` to count actual datasets retrieved
- Quick summary of table structure
- View detailed breakdown with `list()`

### 13. download_analyze.py

Download and analyze resources from NDP datasets.

- Query NDP for datasets with specific keywords
- Filter resources by format (PDF, CSV, etc.)
- Validate resource URLs before downloading
- Download resource files using URLs
- Extract and analyze file metadata (e.g., PDF pages, author)
- Practical example of working with resource URLs

**Demonstrates:**
- Accessing the `resources` table
- Filtering by resource format
- URL validation with HEAD/GET requests
- Downloading files with `requests`
- Extracting metadata from downloaded files using PyPDF2
- Viewing related resources from the same dataset

---

## Working with Resources

Resource tables contain downloadable file metadata. The backend does not automatically download files during metadata retrieval.

### Access Resource URLs

```python
# Get resources table
resources_df = dsi.get_table("resources", collection=True)

# Get CSV resources
csv_resources = resources_df[resources_df['format'] == 'CSV']

for idx, row in csv_resources.iterrows():
    print(f"Name: {row['resource_name']}")
    print(f"URL: {row['url']}")
    print(f"Size: {row['size']} bytes")
    print(f"Dataset: {row['dataset_title']}\n")
```

### Download Resources

```python
import requests

resources_df = dsi.get_table("resources", collection=True)

for idx, row in resources_df.iterrows():
    if row['format'] == 'CSV':
        response = requests.get(row['url'])
        with open(f"{row['resource_name']}", 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {row['resource_name']}")
```

### Filter Resources by Format

```python
# Get all PDF resources
pdf_resources = resources_df[resources_df['format'].str.upper() == 'PDF']
print(f"Found {len(pdf_resources)} PDF resources")

# Get resources from specific dataset
dataset_resources = resources_df[
    resources_df['dataset_title'] == 'My Dataset Title'
]
```

---

## Query Patterns

### Find Datasets by Organization

```python
dsi = DSI(
    backend_name="NDP",
    params={
        "organization": "National Oceanic and Atmospheric Administration",
        "limit": 20
    }
)

datasets_df = dsi.get_table("datasets", collection=True)
print(f"Found {len(datasets_df)} NOAA datasets")
```

### Find Datasets with Specific Tags

```python
dsi = DSI(
    backend_name="NDP",
    params={"tags": ["climate change", "temperature"], "limit": 15}
)
```

### Find Datasets with CSV Data

```python
dsi = DSI(
    backend_name="NDP",
    params={"keywords": "water", "formats": ["CSV"], "limit": 10}
)
```

### Complex Multi-Criteria Search

```python
dsi = DSI(
    backend_name="NDP",
    params={
        "keywords": "air quality",
        "organization": "Environmental Protection Agency",
        "tags": ["pollution"],
        "formats": ["CSV", "JSON"],
        "limit": 30
    }
)
```

---

## Notes

- The backend is **metadata-first** and **read-only**
- Two tables: `datasets` (metadata) and `resources` (files)
- Multiple resource rows may exist for a single dataset
- Resource rows contain metadata and download URLs (files are not downloaded automatically)
- Full API responses are preserved in `raw_dataset` and `raw_resource` fields
- Organization and group names are automatically slugified (lowercase + hyphens) for CKAN queries
- Multi-query support deduplicates results by dataset ID
- Empty result sets return empty DataFrames (no errors)

---

## Troubleshooting

### Empty Results

If your query returns no datasets:

```python
dsi.list()  # Check if tables exist
# Output: datasets: (0 rows, 13 cols)
```

Try:
- Broadening search terms
- Removing filters
- Checking organization name spelling
- Verifying tag names exist in NDP

### Finding Valid Organizations

```python
# Query broadly first
dsi = DSI(backend_name="NDP", params={"keywords": "climate", "limit": 50})
datasets_df = dsi.get_table("datasets", collection=True)

# View available organizations
orgs = datasets_df['organization'].value_counts()
print(orgs)
```

### Finding Valid Tags

```python
datasets_df = dsi.get_table("datasets", collection=True)

# Extract all tags
all_tags = []
for tag_str in datasets_df['tags']:
    if tag_str:
        all_tags.extend(tag_str.split(','))

# View unique tags
unique_tags = set(tag.strip() for tag in all_tags)
print(sorted(unique_tags))
```

### Finding Available Formats

```python
resources_df = dsi.get_table("resources", collection=True)

# View available formats
formats = resources_df['format'].value_counts()
print(formats)
```

---

## Advanced Features

### Validate Resource URLs

Check if resource URLs are accessible:

```python
dsi.validate_urls()

# Access url_valid column
resources_df = dsi.get_table("resources", collection=True)
valid_resources = resources_df[resources_df['url_valid'] == True]
```

### Count Datasets

Count the number of datasets loaded:

```python
num_datasets = dsi.num_datasets()
print(f"Loaded {num_datasets} datasets")
```

### Process to Writable Backend

Convert read-only NDP data to a local database:

```python
# Load NDP data
dsi = DSI(backend_name="NDP", params={"keywords": "climate", "limit": 20})

# Process to local Sqlite database
dsi.process(backend_name="Sqlite", filename="ndp_cache.db")

# Now you can query with SQL
result = dsi.query("SELECT * FROM datasets WHERE num_resources > 10", collection=True)
```

### Export Data

Write tables to external formats:

```python
# Export datasets to CSV
dsi.write(
    filename="datasets.csv",
    writer_name="Csv",
    table_name="datasets"
)

# Export resources to Parquet
dsi.write(
    filename="resources.pq",
    writer_name="Parquet",
    table_name="resources"
)
```

---

## Performance Tips

- Use `limit` parameter to control result size
- Start with broad queries, then refine
- Cache results locally using `process()` for repeated analysis
- Filter DataFrames after retrieval for complex conditions
- Use `display_cols` parameter in `display()` to focus on relevant columns