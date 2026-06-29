# RCSBPDB Backend for DSI

The RCSBPDB backend is a read-only DSI backend for retrieving metadata from the RCSB Protein Data Bank (RCSB PDB).

RCSB PDB is the primary public repository for experimentally determined three-dimensional structures of proteins, nucleic acids, and biological macromolecular complexes. This backend retrieves metadata through the RCSB REST APIs, normalizes the returned information, and exposes it as DSI-compatible tables.

Useful RCSB PDB resources:

- [RCSB PDB](https://www.rcsb.org/): Main structure catalog for searching, viewing, and downloading experimentally determined 3D biological macromolecular structures.
- [RCSB PDB Data API](https://data.rcsb.org/): API documentation for retrieving structure, assembly, entity, citation, and related metadata.
- [RCSB PDB Web APIs Overview](https://www.rcsb.org/docs/programmatic-access/web-apis-overview): Overview of programmatic access, including Search API and Data API workflows.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote RCSB data.

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="RCSBPDB",
    params={"keywords": "hemoglobin", "limit": 5}
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

## Supported Search and Lookup Parameters

The backend supports a single `params` interface.

### Keyword Search

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={"keywords": "genomics", "limit": 10}
)
```

### Author Search

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={"authors": "Kleywegt", "limit": 10}
)
```

### Experimental Method Search

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={"experimental_method": "X-RAY DIFFRACTION", "limit": 10}
)
```

### PDB ID Lookup

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={"pdb_id": "1CBS"}
)
```

### DOI Lookup

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={"DOI": "10.2210/pdb1cbs/pdb"}
)
```

### Multiple Identifiers

```python
dsi = DSI(
    backend_name="RCSBPDB",
    params={
        "identifiers": [
            "1CBS",
            "10.2210/pdb4hhb/pdb"
        ]
    }
)
```

---

## Supported Parameters

The backend supports the following parameters through the unified `params` interface.

| Parameter | Description |
|------------|------------|
| keywords | Full-text search |
| authors | Author search |
| experimental_method | Experimental method search |
| pdb_id/ pdbID/ pdbId/ PDB_ID/ pdbid/ PDBID | PDB ID lookup |
| doi/ DOI | DOI lookup |
| identifiers | Multiple PDB IDs and/or DOIs |
| limit | Maximum number of results |

---

## Tables

The backend returns three DSI tables:

1. `datasets`
2. `resources`
3. `errors`

---

### datasets (Tier 1)

The `datasets` table contains one row per resolved RCSB PDB dataset.

Table fields are a subset of the following properties:

| Column              | Description                                                                               |
| ------------------- | ----------------------------------------------------------------------------------------- |
| dataset_id          | PDB ID                                                                                    |
| doi                 | DOI if available                                                                          |
| title               | Structure title                                                                           |
| description         | Structure description                                                                     |
| landing_page        | RCSB structure URL                                                                        |
| metadata_url        | API metadata URL                                                                          |
| experimental_method | Experimental method                                                                       |
| release_date        | Initial release date                                                                      |
| revision_date       | Latest revision date                                                                      |
| resource_count      | Number of associated resource entries                                                     |
| raw_metadata        | Curated metadata plus full API response                                                   |
| notes               | Additional messages for skipped or failed lookups; typically empty for successful entries |


Example:

```python
datasets_df = dsi.get_table("datasets", collection=True)

print(
    datasets_df[
        [
            "dataset_id",
            "title",
            "experimental_method",
            "resource_count"
        ]
    ]
)
```

---

### resources (Tier 2)

The `resources` table contains downloadable file-based or file-specific metadata associated with each dataset. Each row corresponds to a single downloadable resource. The backend does not download files automatically during metadata retrieval; it follows a search-then-retrieval workflow.

Table fields are a subset of the following properties:

| Column            | Description                |
| ----------------- | -------------------------- |
| resource_id       | Unique resource identifier |
| dataset_id        | Associated PDB ID          |
| name              | Resource file name         |
| download_url      | Download URL               |
| format            | File format                |
| resource_type     | Resource description       |
| source            | Resource source category   |
| raw_metadata      | Resource metadata          |

Example:

```python
resources_df = dsi.get_table("resources", collection=True)

print(
    resources_df[
        [
            "dataset_id",
            "name",
            "format",
            "download_url"
        ]
    ]
)
```

#### Relationship Between Tables

The relationship is:

```text
datasets.dataset_id
          |
          |
          V
resources.dataset_id
```

The `dataset_id` field acts like a primary-key/foreign-key relationship.

A single dataset can have many associated resources.

Example:

```text
datasets
---------
1CBS

resources
---------
1CBS -> mmCIF
1CBS -> PDB
1CBS -> XML
1CBS -> Assembly CIF
1CBS -> Validation Report
...
```

---

### errors

The `errors` table stores failed, skipped, or unresolved lookups.

Table fields are a subset of the following properties:

| Column                | Description         |
| --------------------- | ------------------- |
| identifier            | Original input      |
| normalized_identifier | Normalized value    |
| repo                  | Repository          |
| status                | Error status        |
| endpoint_used         | API endpoint        |
| endpoint_variables    | Endpoint parameters |
| notes                 | Error details       |

If no lookup errors occur, the `errors` table may contain a single blank placeholder row so the table schema can be written safely during SQLite processing.

Example error row:

| identifier | status | notes |
|------------|--------|-------|
| NOT_A_PDB_ID | skipped | Identifier did not match a supported DOI or 4-character PDB ID |

---

## Curated Metadata and Full Metadata

The backend stores metadata at two levels.

### Curated Metadata

Frequently used metadata fields are extracted into table columns for:

* filtering
* searching
* summarization
* reporting
* table joins

These fields appear directly in:

* datasets
* resources
* errors

---

### Full Metadata

The complete RCSB Data API response is preserved in the `raw_metadata` field.

Example:

```python
datasets_df = dsi.get_table("datasets", collection=True)

full_metadata = datasets_df.iloc[0]["raw_metadata"]
```

This ensures:

* no metadata loss
* future compatibility
* access to the original API metadata
* support for advanced workflows

---

## Common DSI Operations

### List Tables

```python
dsi.list()
```

### View Backend Summary

```python
dsi.summary()
```

### Retrieve a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
```

### Search Loaded Row Data

```python
dsi.search("X-RAY")
```

### Find Rows by Column Condition

```python
dsi.find("doi = 10.2210/pdb4hhb/pdb")
```

---

## Example Scripts

The following example scripts demonstrate common workflows.

### 1.load_basic.py

Initialize the RCSBPDB backend using a keyword search and demonstrate the core DSI inspection utilities:

- `dsi.list()`
- `dsi.summary()`

This example provides a quick overview of the available tables and their contents.

### 2.list_tables.py

Retrieve the curated DSI tables using `get_table()` and display them as Pandas DataFrames.

This example demonstrates:

- Accessing the `datasets` table
- Accessing the `resources` table
- Accessing the `errors` table
- Viewing selected metadata fields in a tabular format

### 3.filter_data.py

Filter the Tier 2 `resources` table by file format and use the associated `download_url` values to download matching files.

This example demonstrates:

- Resource-level filtering
- Accessing downloadable file metadata
- Using `download_url` paths
- Downloading selected resources to a local directory

### 4.get_columns.py

Read PDB IDs and DOIs from a CSV file, retrieve metadata, display selected dataset and resource information, export results to CSV, and store the loaded tables in SQLite.

This example demonstrates:

- CSV-driven identifier input
- PDB ID and DOI resolution
- DSI `find()`
- DSI `search()`
- DSI `display()`
- Dataset metadata retrieval
- Resource metadata and download path retrieval
- Resource count summaries by dataset
- DSI `write()` export to CSV
- DSI `process("sqlite", "data.db")` storage workflow

The script creates `pdb_inputs.csv` if it does not already exist.

Expected output files:

- `pdb_inputs.csv`
- `datasets.csv`
- `data.db`

### 5.error_handling.py

Load a mixture of valid and invalid identifiers to demonstrate how failed or skipped lookups are represented in the `errors` table.

This example demonstrates:

- Valid PDB ID lookup
- Invalid identifier handling
- Accessing the `errors` table
- Interpreting `status` and `notes` values for skipped records

---

## Notes

* The backend is metadata-first and read-only.
* The `datasets` table is the Tier 1 table.
* The `resources` table is the Tier 2 table.
* Multiple resource rows may exist for a single dataset.
* Resource rows describe downloadable file-based or file-specific metadata associated with each dataset. Files are not downloaded automatically during metadata retrieval; use the `download_url` values for explicit download workflows.
* Full API metadata is preserved in the `raw_metadata` field.
* If no lookup errors occur, the `errors` table may be empty or may contain a single blank placeholder row for SQLite compatibility.
* RCSB-style DOI values are automatically converted into PDB IDs when possible.
* Curated tables provide simplified access while preserving the complete original metadata.
