# RCSBPDB Backend for DSI

The RCSBPDB backend is a read-only DSI backend for retrieving metadata from the RCSB Protein Data Bank (RCSB PDB).

RCSB PDB is the primary public repository for experimentally determined three-dimensional structures of proteins, nucleic acids, and biological macromolecular complexes. This backend retrieves metadata through the RCSB REST APIs, normalizes the returned information, and exposes it as DSI-compatible tables.

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

The backend supports a single `params` interface regardless of how users search.

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
    params={"experimental_method": "X-RAY DIFFRACTION"}
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

### Search Parameters

* `keywords`
* `authors`
* `experimental_method`
* `limit`
* `start`

### Identifier Parameters

* `pdb_id`
* `pdbID`
* `pdbId`
* `PDB_ID`
* `pdbid`
* `PDBID`
* `doi`
* `DOI`
* `identifiers`

---

## Backend Execution Flow

### Search-Driven Flow

Keyword, author, and experimental method searches follow:

```text
User Search Parameters
        ↓
RCSB Search API
        ↓
PDB IDs
        ↓
RCSB Data API
        ↓
Normalized DSI Tables
```

### Identifier-Driven Flow

PDB IDs and supported DOI values follow:

```text
PDB ID / DOI
        ↓
Identifier Normalization
        ↓
RCSB Data API
        ↓
Normalized DSI Tables
```

Only RCSB-style DOIs of the form:

```text
10.2210/pdbXXXX/pdb
```

are directly converted into PDB IDs.

General publication DOI searches are not currently supported.

---

## Tables

The backend returns three DSI tables:

1. `datasets`
2. `resources`
3. `errors`

---

### datasets (Tier 1)

The `datasets` table contains one row per RCSB PDB structure.

Typical columns include:

| Column              | Description                             |
| ------------------- | --------------------------------------- |
| dataset_id          | PDB ID                                  |
| source_repository   | Source repository name                  |
| doi                 | DOI if available                        |
| title               | Structure title                         |
| description         | Structure description                   |
| landing_page        | RCSB structure URL                      |
| metadata_url        | API metadata URL                        |
| experimental_method | Experimental method                     |
| release_date        | Initial release date                    |
| revision_date       | Latest revision date                    |
| resource_count      | Number of associated resource entries   |
| usability_label     | Resource usability classification       |
| api_status          | Lookup status                           |
| query_source        | How the entry was retrieved             |
| raw_metadata        | Curated metadata plus full API response |
| notes               | Lookup notes                            |

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

The `resources` table contains downloadable file metadata and paths associated with each structure.

Each row corresponds to a single downloadable resource.

Typical columns include:

| Column            | Description                |
| ----------------- | -------------------------- |
| resource_id       | Unique resource identifier |
| dataset_id        | Associated PDB ID          |
| source_repository | Source repository          |
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

Typical columns include:

| Column                | Description         |
| --------------------- | ------------------- |
| identifier            | Original input      |
| normalized_identifier | Normalized value    |
| repo                  | Repository          |
| status                | Error status        |
| endpoint_used         | API endpoint        |
| endpoint_variables    | Endpoint parameters |
| query_source          | Query source        |
| notes                 | Error details       |

An empty errors table typically indicates all lookups succeeded.

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

The complete RCSB Data API response is preserved inside:

```python
raw_metadata["full_metadata"]
```

Example:

```python
datasets_df = dsi.get_table("datasets", collection=True)

full_metadata = datasets_df.iloc[0]["raw_metadata"]["full_metadata"]
```

This ensures:

* no metadata loss
* future compatibility
* access to all original API fields
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

### Search Loaded Metadata

```python
dsi.search("X-RAY")
```

---

## Example Scripts

The following example scripts demonstrate common workflows.

### 1.load_basic.py

Initialize the backend using a keyword search and preview the datasets table.

### 2.list_tables.py

Perform a PDB ID lookup and display table summaries.

### 3.get_table.py

Perform a DOI lookup and retrieve the datasets, resources, and errors tables.

### 4.search.py

Perform an author search and preview the returned datasets.

### 5.filter_data.py

Retrieve Solution NMR entries and summarize the returned datasets by count, experimental method, and resource count.

### 6.get_columns.py

Read PDB IDs and DOIs from an Excel spreadsheet, retrieve metadata, and display all associated resource metadata and paths.

---

## Notes

* The backend is metadata-first and read-only.
* The `datasets` table is the Tier 1 table.
* The `resources` table is the Tier 2 table.
* Multiple resource rows may exist for a single dataset.
* Resource rows contain metadata and download paths, not downloaded file contents.
* Full API responses are preserved under:

```python
raw_metadata["full_metadata"]
```

* Empty `errors` tables typically indicate successful lookups.
* RCSB-style DOI values are automatically converted into PDB IDs when possible.
* Curated tables provide simplified access while preserving the complete original metadata.
