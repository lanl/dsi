# NDP Backend for DSI

The NDP backend is a read-only DSI backend for retrieving metadata from NDP (National Data Platform) CKAN-based data catalogs.

NDP provides open access to datasets from government agencies, research institutions, and other organizations. This backend retrieves metadata through the CKAN API, normalizes the returned information, and exposes it as DSI-compatible tables.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote NDP data.

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    keywords="climate",
    limit=10
)
```

### List Available Tables

```python
dsi.list()
```

### Access a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
print(datasets_df.head())
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

The backend supports flexible querying through a unified `params` interface or direct keyword arguments.

### Keyword Search

Search across dataset titles, descriptions, and tags:

```python
dsi = DSI(
    backend_name="NDP",
    keywords="water quality",
    limit=20
)
```

### Organization Filter

Filter datasets by publishing organization:

```python
dsi = DSI(
    backend_name="NDP",
    organization="U.S. Geological Survey",
    limit=15
)
```

**Note:** Organization names with spaces are automatically handled. Use the exact organization name as it appears in NDP.

### Tag Filter

Filter datasets by one or more tags:

```python
dsi = DSI(
    backend_name="NDP",
    tags=["hydrology", "water"],
    limit=10
)
```

### Format Filter

Filter datasets by resource file formats:

```python
dsi = DSI(
    backend_name="NDP",
    formats=["CSV", "GeoJSON"],
    limit=10
)
```

Common formats include: `CSV`, `JSON`, `GeoJSON`, `XML`, `PDF`, `ZIP`, `SHP` (shapefile)

### Combined Parameters

Combine multiple search criteria:

```python
dsi = DSI(
    backend_name="NDP",
    keywords="climate",
    organization="National Oceanic and Atmospheric Administration",
    tags=["temperature", "ocean"],
    formats=["CSV"],
    limit=25
)
```

### Multiple Independent Queries

Run multiple queries and combine results (deduplicates by dataset ID):

```python
dsi = DSI(
    backend_name="NDP",
    params=[
        {"keywords": "earthquake", "limit": 10},
        {"organization": "USGS", "limit": 10},
        {"tags": ["seismic"], "limit": 10}
    ]
)
```

---

## Supported Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `keywords` | str | Full-text search across titles, descriptions, tags |
| `organization` | str | Filter by publishing organization name |
| `tags` | list[str] | Filter by one or more tags (AND logic) |
| `formats` | list[str] | Filter by resource file formats (OR logic) |
| `limit` | int | Maximum number of datasets to retrieve per query (default: 100) |
| `params` | dict or list[dict] | Dictionary or list of query parameter dicts (enables multi-query) |

---

## Tables

The backend returns two primary DSI tables plus per-dataset resource tables:

1. `datasets` (Tier 1 - Dataset metadata)
2. Per-dataset resource tables (Tier 2 - One table per dataset containing its resources)

---

### datasets (Tier 1)

The `datasets` table contains one row per NDP dataset.

| Column | Description |
|--------|-------------|
| id | Unique dataset identifier (CKAN package ID) |
| name | Machine-readable dataset name |
| title | Human-readable dataset title |
| notes | Dataset description/abstract |
| organization | Publishing organization name |
| author | Dataset author/creator |
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

### Resource Tables (Tier 2)

Each dataset has a corresponding resource table containing downloadable files and metadata.

Table name format: `{dataset_title}`

You can access these tables by:
- **Dataset title** (human-readable)
- **Dataset ID** (machine-readable UUID)

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
| raw_dataset | Full CKAN API response for the resource |

**Example:**

```python
# List all available resource tables
table_names = dsi.list(collection=True)
resource_tables = [t for t in table_names if t != 'datasets']

# Access first dataset's resources
if resource_tables:
    resources_df = dsi.get_table(resource_tables[0], collection=True)
    print(resources_df[["resource_name", "format", "url"]])
```

#### Relationship Between Tables

```text
datasets.id
    |
    |
    V
resources.dataset_id
```

The `id` field in `datasets` matches `dataset_id` in resource tables.

**Example:**

```text
datasets
---------
Dataset: "Water Quality Measurements 2023"
  id: abc-123-def

Resource Table: "Water Quality Measurements 2023"
---------
resource_1 -> dataset_id: abc-123-def -> CSV
resource_2 -> dataset_id: abc-123-def -> JSON
resource_3 -> dataset_id: abc-123-def -> PDF
```

---

## Metadata Storage

### Curated Metadata

Frequently used metadata fields are extracted into table columns for easy access:

- Filtering
- Searching  
- Summarization
- Reporting

These appear directly in `datasets` and resource table columns.

### Full Metadata

The complete CKAN API response is preserved in:

```python
raw_dataset  # Full API response
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
datasets: (10 rows, 12 cols)
Water Quality Data 2023: (3 rows, 8 cols)
Climate Observations: (5 rows, 8 cols)
...
```

### View Backend Summary

```python
dsi.summary()
```

### Retrieve a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
```

### Search Loaded Metadata

```python
dsi.search("CSV")  # Find all mentions of "CSV" across tables
```

### Display Table Preview

```python
dsi.display("datasets", num_rows=5)
```

### Filter with Pandas

```python
datasets_df = dsi.get_table("datasets", collection=True)
filtered = datasets_df[datasets_df['num_resources'] > 5]
print(filtered[['title', 'num_resources']])
```

---

## Example Scripts

The following example scripts demonstrate common workflows.

### **1.load_basic.py**

Initialize the NDP backend with a simple keyword search:

- `dsi.list()` to view tables
- Basic backend inspection

### **2.list_tables.py**

Access the `datasets` table:

- Retrieve as DataFrame
- View table names
- Display table dimensions

### **3.get_table.py**

Work with both datasets and resource tables:

- Access `datasets` table
- Access resource tables by name
- View table structure and columns

### **4.search.py**

Search across all loaded tables:

- Find mentions of specific terms
- Return results as DataFrames
- Navigate multi-table search results

### **5.filter_data.py**

Filter datasets using Pandas queries:

- Query by `num_resources`
- Group by `organization`
- Filter by specific criteria

### **6.org_tag_multiple.py** ⭐ NEW

Query by organization AND tags:

- Combine multiple parameters
- Filter by organization
- Filter by tags simultaneously
- View dataset details

### **7.format_filter.py** ⭐ NEW

Filter datasets by resource formats:

- Find datasets with CSV resources
- Find datasets with GeoJSON resources
- View resource format breakdowns
- Access downloadable file metadata

### **8.multiple_queries.py** ⭐ NEW

Run multiple independent queries:

- Query different organizations
- Query different topics
- Combine and deduplicate results
- Summarize multi-query results

---

## Working with Resources

Resource tables contain downloadable file metadata. The backend does not automatically download files during metadata retrieval.

### Access Resource URLs

```python
# Get first dataset's resources
table_names = dsi.list(collection=True)
resource_tables = [t for t in table_names if t != 'datasets']

if resource_tables:
    resources_df = dsi.get_table(resource_tables[0], collection=True)
    
    # Get CSV resources
    csv_resources = resources_df[resources_df['format'] == 'CSV']
    
    for idx, row in csv_resources.iterrows():
        print(f"Name: {row['resource_name']}")
        print(f"URL: {row['url']}")
        print(f"Size: {row['size']} bytes\n")
```

### Download Resources

```python
import requests

resources_df = dsi.get_table(resource_tables[0], collection=True)

for idx, row in resources_df.iterrows():
    if row['format'] == 'CSV':
        response = requests.get(row['url'])
        with open(f"{row['resource_name']}", 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {row['resource_name']}")
```

---

## Query Patterns

### Find Datasets by Organization

```python
dsi = DSI(
    backend_name="NDP",
    organization="National Oceanic and Atmospheric Administration",
    limit=20
)

datasets_df = dsi.get_table("datasets", collection=True)
print(f"Found {len(datasets_df)} NOAA datasets")
```

### Find Datasets with Specific Tags

```python
dsi = DSI(
    backend_name="NDP",
    tags=["climate change", "temperature"],
    limit=15
)
```

### Find Datasets with CSV Data

```python
dsi = DSI(
    backend_name="NDP",
    keywords="water",
    formats=["CSV"],
    limit=10
)
```

### Complex Multi-Criteria Search

```python
dsi = DSI(
    backend_name="NDP",
    keywords="air quality",
    organization="Environmental Protection Agency",
    tags=["pollution"],
    formats=["CSV", "JSON"],
    limit=30
)
```

---

## Notes

- The backend is **metadata-first** and **read-only**
- The `datasets` table is the **Tier 1** table
- Resource tables are **Tier 2** tables (one per dataset)
- Multiple resource rows may exist for a single dataset
- Resource rows contain metadata and download URLs (files are not downloaded automatically)
- Full API responses are preserved in `raw_dataset` fields
- Organization names with spaces are automatically quoted for CKAN queries
- Multi-query support deduplicates results by dataset ID
- Empty result sets return empty DataFrames (no errors)

---

## Troubleshooting

### Empty Results

If your query returns no datasets:

```python
dsi.list()  # Check if tables exist
# Output: datasets: (0 rows, 12 cols)
```

Try:
- Broadening search terms
- Removing filters
- Checking organization name spelling
- Verifying tag names exist in NDP

### Finding Valid Organizations

```python
# Query broadly first
dsi = DSI(backend_name="NDP", keywords="climate", limit=50)
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

---

## API Reference

For developers, the backend uses the CKAN API:

- Base URL: `https://nationaldataplatform.org/catalog`
- API version: 3
- Endpoint: `/api/3/action/package_search`

Query parameters are automatically formatted and validated by the backend.