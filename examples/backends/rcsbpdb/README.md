# RCSBPDB Backend for DSI

The RCSBPDB backend is a read-only DSI backend for retrieving metadata from the RCSB Protein Data Bank (RCSB PDB).

RCSB PDB is the primary public repository for experimentally determined three-dimensional structures of proteins, nucleic acids, and biological macromolecular complexes. This backend retrieves metadata through the RCSB REST APIs, normalizes the returned information, and exposes it as DSI-compatible tables.

Useful RCSB PDB resources:

- [RCSB PDB](https://www.rcsb.org/): Main structure catalog for searching, viewing, and downloading experimentally determined 3D biological macromolecular structures.
- [RCSB PDB Data API](https://data.rcsb.org/): API documentation for retrieving structure, assembly, entity, citation, and related metadata.
- [RCSB PDB Web APIs Overview](https://www.rcsb.org/docs/programmatic-access/web-apis-overview): Overview of programmatic access, including Search API and Data API workflows.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote RCSB data.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses the RCSB Search API to discover PDB entries and the RCSB Data API to retrieve metadata for a selected PDB entry.

- Search API endpoint: `https://search.rcsb.org/rcsbsearch/v2/query`
- Data API base URL: `https://data.rcsb.org/rest/v1/core`
- Entry metadata endpoint: `/entry/{pdb_id}`
- Structure landing page: `https://www.rcsb.org/structure/{pdb_id}`
- Search request method: `POST`
- Entry metadata request method: `GET`

Tiered workflow:

```text
DSI initialization
    -> validates API connection and creates an empty cache

Tier 1 dictionary query
    -> RCSB Search API
    -> matching PDB IDs
    -> one dataset-discovery DataFrame and one cached table

Tier 2 dictionary query
    -> selected PDB ID or DOI
    -> RCSB Data API
    -> one resource-pointer DataFrame and one cached table
```

</details>

---

## Quick Start

### Initialize the Backend

Initialization does not retrieve remote data. It only validates the RCSB connection and creates an empty cache.

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="RCSBPDB",
    silence_messages=True,
)
```

### Discover Datasets (Tier 1)

```python
datasets_df = dsi.query(
    {"keywords": "hemoglobin", "limit": 5},
    collection=True,
)

datasets_table = datasets_df.attrs["table_name"]
print(datasets_df.head())
```

### Retrieve Resources for a Selected Entry (Tier 2)

```python
selected_pdb_id = datasets_df.iloc[0]["dataset_id"]

resources_df = dsi.query(
    {"pdb_id": selected_pdb_id},
    collection=True,
)

resources_table = resources_df.attrs["table_name"]
print(resources_df.head())
```

### List Available Tables

```python
dsi.list()
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search and Lookup Parameters

RCSBPDB accepts dictionary parameters through `dsi.query(..., collection=True)`, not through `DSI(...)` initialization. Every dictionary query returns exactly one Pandas DataFrame and appends exactly one ordered table to the backend cache.

### Keyword Search

Search RCSB PDB metadata using a full-text keyword and limit the number of returned entries. This is a Tier 1 discovery query.

```python
datasets_df = dsi.query(
    {"keywords": "genomics", "limit": 10},
    collection=True,
)
```

### Author Search

Search for structures associated with a specific primary-citation author. This is a Tier 1 discovery query.

```python
datasets_df = dsi.query(
    {"authors": "Kleywegt", "limit": 10},
    collection=True,
)
```

### Experimental Method Search

Filter structures by an exact experimental method. This is a Tier 1 discovery query.

```python
datasets_df = dsi.query(
    {"experimental_method": "X-RAY DIFFRACTION", "limit": 10},
    collection=True,
)
```

### PDB ID Lookup

Retrieve downloadable resource pointers for a specific four-character PDB ID. This is a Tier 2 query.

```python
resources_df = dsi.query(
    {"pdb_id": "1CBS"},
    collection=True,
)
```

### DOI Lookup

Resolve an RCSB-style DOI to its PDB ID and retrieve downloadable resource pointers. This is a Tier 2 query.

```python
resources_df = dsi.query(
    {"DOI": "10.2210/pdb1cbs/pdb"},
    collection=True,
)
```

### Multiple Identifiers

Retrieve resource pointers for multiple structures using a combination of PDB IDs and RCSB-style DOIs.

```python
resources_df = dsi.query(
    {
        "identifiers": [
            "1CBS",
            "10.2210/pdb4hhb/pdb",
        ]
    },
    collection=True,
)
```

---

## Supported Parameters

The backend supports the following parameters through dictionary queries.

| Parameter | Description |
|------------|-------------|
| keywords | Full-text search |
| authors | Author search |
| experimental_method | Experimental method search |
| pdb_id / pdbID / pdbId / PDB_ID / pdbid / PDBID | PDB ID lookup |
| doi / DOI | DOI lookup |
| identifiers | Multiple PDB IDs and/or DOIs |
| limit | Maximum number of Tier 1 discovery results |
| start | Starting offset for Tier 1 discovery results |
| return_type | RCSB Search API result type; defaults to `entry` |

---

## Tables

Each dictionary query returns one DataFrame and adds one table to the cache. Table names are dynamic and are available through `result_df.attrs["table_name"]`.

Typical names are:

1. `pdb_datasets_<query>` for Tier 1 dataset discovery.
2. `pdb_resources_<pdb_id>` for Tier 2 selected-entry resources.
3. `pdb_resources_query_errors` or `pdb_datasets_<query>_errors` when a query produces only errors.

---

### Dataset Discovery (Tier 1)

A Tier 1 DataFrame contains one row per matching RCSB PDB entry. Its table name begins with `pdb_datasets_`.

| Column | Description |
| --- | --- |
| dataset_id | PDB ID |
| doi | DOI if available |
| title | Structure title |
| description | Structure description |
| landing_page | RCSB structure URL |
| metadata_url | API metadata URL |
| experimental_method | Experimental method |
| release_date | Initial release date |
| revision_date | Latest revision date |
| resource_count | Not loaded during Tier 1 discovery |
| raw_metadata | Curated metadata plus full API response |
| notes | Additional messages for skipped or failed lookups |

Example:

```python
datasets_df = dsi.query(
    {"keywords": "hemoglobin", "limit": 5},
    collection=True,
)

print(
    datasets_df[
        ["dataset_id", "title", "experimental_method", "resource_count"]
    ]
)
```

---

### Resource Pointers (Tier 2)

A Tier 2 DataFrame contains downloadable file-based metadata for selected PDB entries. Its table name begins with `pdb_resources_`. The backend does not download files automatically.

| Column | Description |
| --- | --- |
| resource_id | Unique resource identifier |
| dataset_id | Associated PDB ID |
| name | Resource file name |
| download_url | Download URL |
| format | File format |
| resource_type | Resource description |
| source | Resource source category |
| raw_metadata | Resource metadata |

Example:

```python
resources_df = dsi.query(
    {"pdb_id": "1CBS"},
    collection=True,
)

print(
    resources_df[
        ["dataset_id", "name", "format", "download_url"]
    ]
)
```

#### Relationship Between Results

The shared field is:

```text
dataset discovery dataset_id
             |
             v
resource pointer dataset_id
```

A selected dataset can have many resource-pointer rows.

---

### Error Results

If a dictionary query has no successful result, `query_artifacts()` returns and caches one error DataFrame. Its table name ends with `_errors`.

| Column | Description |
| --- | --- |
| identifier | Original input |
| normalized_identifier | Normalized value |
| repo | Repository |
| status | Error status |
| endpoint_used | API endpoint |
| endpoint_variables | Endpoint parameters |
| notes | Error details |

Example:

```python
errors_df = dsi.query(
    {"identifiers": ["NOT_A_PDB_ID"]},
    collection=True,
)
```

---

## Curated Metadata and Full Metadata

The backend stores metadata at two levels.

### Curated Metadata

Frequently used metadata fields are extracted into DataFrame columns for:

- filtering
- searching
- summarization
- reporting
- selection of a PDB entry for Tier 2 retrieval

These fields appear in the returned discovery, resource, or error DataFrame.

---

### Full Metadata

The complete RCSB Data API response is preserved in the `raw_metadata` field when it is available.

Example:

```python
datasets_df = dsi.query(
    {"keywords": "hemoglobin", "limit": 5},
    collection=True,
)

full_metadata = datasets_df.iloc[0]["raw_metadata"]
```

This ensures:

- no metadata loss
- future compatibility
- access to the original API metadata
- support for advanced workflows

---

## Common DSI Operations

### List Cached Tables

```python
dsi.list()
```

### View Backend Summary

```python
dsi.summary()
```

### Retrieve a Cached Table

```python
datasets_df = dsi.query(
    {"keywords": "hemoglobin", "limit": 5},
    collection=True,
)

datasets_table = datasets_df.attrs["table_name"]
cached_df = dsi.get_table(datasets_table, collection=True)
```

### Search Cached Row Data

Use a nonempty string query after data has been cached.

```python
dsi.query("X-RAY")
```

### Find Cached Rows by Column Condition

```python
dsi.query("doi = 10.2210/pdb4hhb/pdb")
```

---

## Example Scripts

The following example scripts demonstrate the tiered query workflow.

### 1.load_basic.py

Initialize the empty RCSBPDB backend, run a Tier 1 keyword discovery query, and demonstrate the core DSI inspection utilities:

- `dsi.query({...}, collection=True)`
- `dsi.list()`
- `dsi.summary()`

### 2.list_tables.py

Run a Tier 1 dataset-discovery query, choose one returned PDB ID, then run a Tier 2 resource query. The example displays selected columns from both returned DataFrames.

### 3.filter_data.py

Run a discovery query, select an entry, retrieve Tier 2 resources, filter them by file format, and use their `download_url` values to download selected files.

### 4.get_columns.py

Read PDB IDs and DOIs from a CSV file, run a Tier 2 resource query, inspect cached data using `dsi.query()` string searches, export the returned resource table to CSV, and store cached results in SQLite.

The script creates `pdb_inputs.csv` if it does not already exist.

Expected output files:

- `pdb_inputs.csv`
- `resources.csv`
- `data.db`

### 5.error_handling.py

Run an invalid identifier query to demonstrate the single returned and cached error table, then use a string relation query to find skipped records.

---

## Notes

- The backend is metadata-first and read-only.
- `DSI(backend_name="RCSBPDB")` validates the remote connection and creates an empty `OrderedDict` cache; it never retrieves remote records.
- A dictionary query returns one Pandas DataFrame and appends one ordered table to the cache.
- Tier 1 discovery returns high-level dataset metadata. Tier 2 selection returns downloadable resource pointers.
- Cache table names are dynamic; get them from `result_df.attrs["table_name"]` or `dsi.list(collection=True)`.
- Resource rows describe downloadable file-based or file-specific metadata. Files are not downloaded automatically; use `download_url` values for explicit download workflows.
- Full API metadata is preserved in the `raw_metadata` field when available.
- RCSB-style DOI values are automatically converted into PDB IDs when possible.
- String queries explore data already cached locally; they do not make a new remote RCSB API request.
