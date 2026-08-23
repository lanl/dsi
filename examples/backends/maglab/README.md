# Maglab Backend for DSI

A read-only backend for accessing MagLab-published datasets hosted on the Open Science Framework (OSF) via the OSF REST API. Metadata is retrieved and exposed as DSI-compatible tables: `datasets`, `files`, and `relationships`.

The National High Magnetic Field Laboratory (MagLab) publishes experimental datasets on OSF, a free and open platform for research data, code, and materials. This backend retrieves node (project), file, and relationship metadata through the OSF API, normalizes the returned information, and exposes it as DSI-compatible tables.

Useful OSF resources:

- [OSF](https://osf.io): Main platform for browsing, hosting, and sharing research data, including MagLab-published datasets.
- [OSF API Documentation](https://developer.osf.io/): Official API reference for querying and retrieving OSF node, file, and relationship metadata.
- [Example Maglab Dataset](https://osf.io/8r2b3/overview): A public MagLab dataset hosted on OSF, used throughout these examples.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote OSF data.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses the OSF v2 REST API with the following endpoints:

- Base URL: `https://api.osf.io/v2`
- Node metadata endpoint: `/nodes/{node_id}/` — returns attributes, relationships, and links for a single OSF node.
- Node file providers endpoint: `/nodes/{node_id}/files/` — lists storage providers (e.g. `osfstorage`) attached to a node.
- Provider file listing — the `related` link returned for a storage provider, recursively walked (depth-first through folders) to enumerate files. Pagination is handled via `page[size]` (100 per page) and OSF's `links.next` cursor.

Query parameters are automatically formatted by the backend based on the `params` dictionary passed to `DSI(backend_name="Maglab", params=...)`.

</details>

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3"}
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

The backend supports querying OSF nodes through a unified `params` interface.

### Basic Node Lookup

Retrieve metadata, files, and relationships for a single OSF node:

```python
dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3"}
)
```

### Storage Provider

Specify a non-default storage provider (defaults to `osfstorage`):

```python
dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3", "provider": "osfstorage"}
)
```

### Filter by File Extension

Only include files with certain extensions:

```python
dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3", "include_ext": ".tdms"}
)
```

Exclude files with certain extensions:

```python
dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3", "exclude_ext": [".png", ".jpg"]}
)
```

### Multiple Nodes

Pass a list of param dicts to fetch and merge multiple OSF nodes into one `datasets`/`files`/`relationships` table set (results are deduplicated by `node_id` and `osf_file_id`):

```python
dsi = DSI(
    backend_name="Maglab",
    params=[
        {"node_id": "8r2b3"},
        {"node_id": "gvudy"}
    ]
)
```

---

## Supported Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `node_id` | str | Required. OSF node id (e.g. `"8r2b3"`) |
| `provider` | str | Optional. Storage provider (default `"osfstorage"`) |
| `include_ext` | str or list of str | Optional. Only include files with these extensions (e.g. `".tdms"`) |
| `exclude_ext` | str or list of str | Optional. Exclude files with these extensions |

---

## Tables

The backend returns up to three DSI tables:

1. **datasets** - Dataset/node metadata (one row per OSF node)
2. **files** - Combined file metadata from all requested nodes
3. **relationships** - Combined relationship pointers from all requested nodes

---

### datasets Table

The `datasets` table contains one row per OSF node retrieved from OSF.

Table fields include:

| Column | Description |
|--------|-------------|
| node_id | OSF node id |
| title | Node title |
| description | Node description |
| category | OSF category (e.g. `project`) |
| date_created | Node creation date |
| date_modified | Last modification date |
| registration | Whether the node is a registration |
| collection | Whether the node is a collection |
| tags | Node tags |
| analytics_key | OSF analytics key |
| public | Whether the node is public |
| subjects | Subject classifications |
| license | Node license information |
| html | Human-readable OSF page URL |
| self | OSF API self link |
| iri | OSF IRI link |
| raw_attributes | Full parsed JSON response from `nodes/{node_id}/` |

**Example:**

```python
datasets_df = dsi.get_table("datasets", collection=True)
print(datasets_df[["title", "category", "public"]])
```

---

### files Table

The `files` table contains combined file metadata from all requested nodes, gathered by recursively walking each node's storage provider file tree.

Table fields include:

| Column | Description |
|--------|-------------|
| node_id | OSF node id the file belongs to |
| osf_file_id | Unique OSF file identifier |
| name | File name |
| materialized_path | Full path of the file within the storage provider |
| provider | Storage provider name (e.g. `osfstorage`) |
| size_bytes | File size in bytes |
| download_url | Direct download URL |
| raw_attributes | Full file attributes from the OSF API |

**Example:**

```python
files_df = dsi.get_table("files", collection=True)
print(files_df[["name", "size_bytes", "download_url"]])
```

#### Relationship Between Tables

```text
datasets.node_id
    |
    |
    V
files.node_id
```

The `node_id` field in the `datasets` table matches the `node_id` field in the `files` table.

This creates a one-to-many relationship: each dataset (OSF node) can have multiple files, but each file belongs to exactly one requested node.

---

### relationships Table

The `relationships` table contains one row per relationship pointer found on each requested node (e.g. `children`, `parent`, `root`, `contributors`), excluding `files` since that relationship is already expanded into the full `files` table.

Table fields include:

| Column | Description |
|--------|-------------|
| node_id | OSF node id the relationship belongs to |
| relationship_name | Name of the relationship pointer (e.g. `children`, `parent`, `root`) |
| href | Related or self link URL for this relationship |
| has_inline_data | Whether the relationship included inline `data` in the OSF response |

**Example:**

```python
relationships_df = dsi.get_table("relationships", collection=True)
print(relationships_df[["node_id", "relationship_name", "href"]])
```

---

## Metadata

### Curated Metadata

Frequently used metadata fields are extracted into table columns for easy access:

- Filtering
- Searching
- Summarization
- Reporting

These appear directly in `datasets`, `files`, and `relationships` table columns.

### Full Metadata

The complete OSF API response (or file attributes block) is preserved in:

```python
raw_attributes  # Full OSF API response for the dataset row, or full attributes block for the file row
```

**Example:**

```python
datasets_df = dsi.get_table("datasets", collection=True)
full_metadata = datasets_df.iloc[0]["raw_attributes"]
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
  - num of columns: 17
  - num of rows: 1

Table: files
  - num of columns: 8
  - num of rows: 167

Table: relationships
  - num of columns: 4
  - num of rows: 28
```

### View Backend Summary

```python
dsi.summary()  # Shows SQL-style types: INTEGER, TEXT, OBJECT
```

### View Table Schema

```python
print(dsi.schema("files"))  # Shows SQL CREATE TABLE format
```

### Retrieve a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
files_df = dsi.get_table("files", collection=True)
relationships_df = dsi.get_table("relationships", collection=True)
```

### Search Loaded Metadata

```python
# dsi = DSI(
#     backend_name="Maglab",
#     params={"node_id": "8r2b3"}
# )

# Prints ALL cells from rows containing "tdms"
dsi.search("tdms")
```

**Note:** `search()` displays complete matching rows (all columns), not just matched cells. Searches across:
- Table names
- Column names
- Cell values

### Filter Data

```python
# dsi = DSI(
#     backend_name="Maglab",
#     params={"node_id": "8r2b3"}
# )

# Find files larger than 1MB
results = dsi.find("size_bytes > 1000000")
```

Supports operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~` (contains), and `(min, max)` for a range

### Display Table Preview

```python
dsi.display("files", num_rows=10)
```

**Note:** `display()` shows ALL columns by default. Use `display_cols` to limit columns:

```python
dsi.display("files", num_rows=10, display_cols=["name", "size_bytes", "download_url"])
```

### Process to Writable Backend

Convert read-only Maglab data to a local database:

```python
dsi = DSI(
    backend_name="Maglab",
    params={"node_id": "8r2b3"}
)

dsi.process(
    backend_name="Sqlite",
    filename="maglab_data.db"
)
dsi.close()

local_dsi = DSI(
    backend_name="Sqlite",
    filename="maglab_data.db"
)

files = local_dsi.get_table("files", collection=True)
print(f"Loaded {len(files)} files from local database")
local_dsi.close()
```

### Export Data

```python
dsi.write(
    filename="files.csv",
    writer_name="Csv",
    table_name="files"
)
```

---

## Example Scripts

The following example scripts demonstrate common workflows with the Maglab backend. All scripts are in `examples/maglab/`.

### 1. load_basic.py

Initialize the Maglab backend with a single OSF node and view available tables.

- Basic query with `node_id` parameter
- Use `list()` to see available tables
- Use `summary()` to view table statistics
- Introduction to Maglab backend structure

---

### 2. filter_by_extension.py

Filter files by extension using `include_ext` and `exclude_ext`.

- Load the same node with `include_ext=".tdms"`
- Reload with `exclude_ext=".tdms"` for comparison
- Compare row counts between filtered queries
- OSF nodes commonly contain other file types (e.g. `.png`, `.txt`) alongside `.tdms` data files

---

### 3. multiple_nodes.py

Fetch and merge multiple OSF nodes into one set of tables.

- Pass a list of param dicts, each with its own `node_id`
- Results are deduplicated by `node_id` and `osf_file_id`
- Use `list()` to confirm combined table sizes

---

### 4. explore_relationships.py

Explore the `relationships` table to discover how a dataset fits into the broader OSF project structure.

- Load a single node
- Retrieve the `relationships` table with `get_table("relationships", collection=True)`
- Inspect pointers such as `children`, `parent`, and `root` and their `href` values
- Determine whether a dataset is a sub-node of a larger OSF project

---

### 5. find_and_filter.py

Use `find()` and `search()` to filter and search loaded Maglab metadata.

- Filter the `files` table with a numeric condition (`size_bytes > 1000000`)
- Search across all tables for a keyword (e.g. `"tdms"`)
- Demonstrates the `>`, `<`, `==`, `!=`, `~~` operator syntax

---

### 6. download_files.py

Retrieve `.tdms` file metadata and download a couple of files using their `download_url` values.

- Load a node with `include_ext=".tdms"`
- Get the `files` table
- Use `requests` to manually download selected files
- Metadata-first workflow: downloads are a deliberate follow-up step, not automatic

---

### 7. write_and_process.py

Persist Maglab metadata into a local SQLite database and reload it.

- Load data from the Maglab backend
- Use `dsi.process()` to convert to a Sqlite backend
- Reload the new SQLite file with a fresh `DSI` instance
- Query the local database with SQL (not supported directly on the read-only Maglab backend)

---

## Notes

- The backend is **metadata-first** and **read-only**.
- Three tables: `datasets` (node metadata), `files` (file metadata), and `relationships` (relationship pointers).
- Multiple file rows may exist for a single dataset; multiple relationship rows may exist for a single dataset.
- File rows contain metadata and download URLs; files are not downloaded automatically.
- Full API responses are preserved in the `raw_attributes` field on both `datasets` and `files` rows.
- The `files` relationship is excluded from the `relationships` table since it is already expanded into the full `files` table.
- Multi-node queries deduplicate `datasets` rows by `node_id` and `files` rows by `osf_file_id`.
- SQL-style `query()` operations are not supported directly by the Maglab backend; use `find()`, `search()`, or `find_relation()` instead.
- Maglab metadata can be transferred to another DSI backend, such as Sqlite, using `dsi.process()`.
