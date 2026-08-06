# Zenodo Backend for DSI

A read-only backend for accessing public Zenodo records through the Zenodo REST API. Metadata is retrieved from Zenodo and exposed as DSI-compatible tables: `datasets` and `resources`.

Zenodo is a general-purpose research repository for sharing and preserving digital research objects such as datasets, publications, software, presentations, posters, and other research outputs. Zenodo is operated by CERN and was launched through the OpenAIRE program. Published Zenodo records receive persistent Digital Object Identifiers (DOIs), making research outputs easier to cite, preserve, and discover.

This backend retrieves public record metadata from Zenodo, normalizes the returned information, and exposes it as two stable DSI tables.

Useful Zenodo resources:

- [Zenodo](https://zenodo.org/): Main Zenodo repository for searching, browsing, and accessing public research records.
- [Zenodo About](https://about.zenodo.org/): Overview of Zenodo, its mission, history, and infrastructure.
- [Zenodo Help Documentation](https://help.zenodo.org/docs/): User documentation for sharing, publishing, versioning, communities, and records.
- [Zenodo Developers](https://developers.zenodo.org/): Official Zenodo API documentation.
- [Zenodo Records API](https://zenodo.org/api/records): Public API endpoint used by this backend for record search.
- [Zenodo API Root](https://zenodo.org/api/): API root endpoint for Zenodo.

> **Note:** This backend is read-only. It retrieves and organizes public Zenodo metadata but does not create, update, delete, upload, or modify Zenodo records.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses public Zenodo record endpoints:

- Base URL: `https://zenodo.org`
- Records search endpoint: `https://zenodo.org/api/records`
- Single record endpoint: `https://zenodo.org/api/records/{record_id}`

Examples:

```text
GET https://zenodo.org/api/records?q=climate&size=5&page=1
GET https://zenodo.org/api/records/16537543
```

The backend automatically builds request parameters from the user-provided `params` dictionary.

</details>

---

## Quick Start

### Initialize the Backend Directly

```python
from dsi.backends.zenodo import Zenodo

zenodo = Zenodo(
    params={"keywords": "climate", "limit": 5},
    verify_ssl=False
)
```

### Initialize Through DSI

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="Zenodo",
    params={"keywords": "climate", "limit": 5},
    verify_ssl=False
)
```

### List Available Tables

```python
dsi.list()
```

or with the backend directly:

```python
zenodo.list()
```

### Access a Table

```python
datasets_df = dsi.get_table("datasets", collection=True)
resources_df = dsi.get_table("resources", collection=True)

print(datasets_df)
print(resources_df)
```

or with the backend directly:

```python
datasets_df = zenodo.get_table("datasets")
resources_df = zenodo.get_table("resources")

print(datasets_df)
print(resources_df)
```

### Close the Backend

```python
dsi.close()
```

or:

```python
zenodo.close()
```

---

## Supported Search Parameters

The backend supports flexible querying through a unified `params` interface.

### Keyword Search

Search Zenodo records using a free-text query.

```python
zenodo = Zenodo(
    params={"keywords": "climate", "limit": 10},
    verify_ssl=False
)
```

Equivalent DSI usage:

```python
dsi = DSI(
    backend_name="Zenodo",
    params={"keywords": "climate", "limit": 10},
    verify_ssl=False
)
```

### Query String Search

Use Zenodo-style query text through `q`.

```python
zenodo = Zenodo(
    params={"q": "climate change", "limit": 10},
    verify_ssl=False
)
```

### Record ID Lookup

Load a specific Zenodo record by record ID.

```python
zenodo = Zenodo(
    params={"record_id": "16537543"},
    verify_ssl=False
)
```

Convenience constructor:

```python
zenodo = Zenodo(
    record_id="16537543",
    verify_ssl=False
)
```

### DOI Lookup

Load a specific Zenodo record by DOI.

```python
zenodo = Zenodo(
    params={"doi": "10.5281/zenodo.16537543"},
    verify_ssl=False
)
```

Convenience constructor:

```python
zenodo = Zenodo(
    doi="10.5281/zenodo.16537543",
    verify_ssl=False
)
```

### Limit Results

Control the maximum number of records returned from a search.

```python
zenodo = Zenodo(
    params={"keywords": "battery materials", "limit": 3},
    verify_ssl=False
)
```

### Pagination

Use `page` to retrieve a specific result page.

```python
zenodo = Zenodo(
    params={
        "keywords": "climate",
        "limit": 10,
        "page": 2
    },
    verify_ssl=False
)
```

### Sorting

Pass a Zenodo-supported sort value.

```python
zenodo = Zenodo(
    params={
        "keywords": "climate",
        "limit": 10,
        "sort": "mostrecent"
    },
    verify_ssl=False
)
```

### Community Search

Search records associated with a Zenodo community.

```python
zenodo = Zenodo(
    params={
        "keywords": "climate",
        "communities": "ecfunded",
        "limit": 10
    },
    verify_ssl=False
)
```

### Resource Type Search

Filter by Zenodo resource type.

```python
zenodo = Zenodo(
    params={
        "keywords": "climate",
        "resource_type": "dataset",
        "limit": 10
    },
    verify_ssl=False
)
```

### Access Right Search

Filter by access rights.

```python
zenodo = Zenodo(
    params={
        "keywords": "climate",
        "access_right": "open",
        "limit": 10
    },
    verify_ssl=False
)
```

---

## Supported Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `keywords` | str | Free-text search term used to search Zenodo records |
| `q` | str | Explicit Zenodo query string |
| `record_id` | str or list[str] | Zenodo record ID lookup |
| `recordId` | str or list[str] | Alternate record ID key |
| `recordID` | str or list[str] | Alternate record ID key |
| `doi` | str or list[str] | DOI lookup |
| `DOI` | str or list[str] | Alternate DOI key |
| `limit` | int | Maximum number of search results to retrieve |
| `size` | int | Alternate name for `limit` |
| `page` | int | Result page number |
| `sort` | str | Zenodo sort option passed to the API |
| `communities` | str | Filter by Zenodo community |
| `resource_type` | str | Filter by Zenodo resource type |
| `access_right` | str | Filter by Zenodo access rights |

---

## Tables

The backend returns two DSI tables:

1. **datasets** - Record-level Zenodo metadata, one row per Zenodo record.
2. **resources** - File-level Zenodo metadata, one row per file attached to a Zenodo record.

The backend does not create an errors table.

---

## `datasets` Table

The `datasets` table contains one row per Zenodo record.

| Column | Description |
|--------|-------------|
| `dataset_id` | Zenodo record ID |
| `concept_record_id` | Zenodo concept record ID for the version family |
| `doi` | DOI for this specific record version |
| `concept_doi` | Concept DOI for the version family, when available |
| `title` | Record title |
| `description` | Record description or abstract |
| `source_repository` | Source repository name, always `Zenodo` |
| `landing_page` | Human-readable Zenodo landing page URL |
| `metadata_url` | Zenodo API metadata URL |
| `publication_date` | Publication date from Zenodo metadata |
| `resource_type` | Zenodo resource type metadata |
| `access_right` | Access right, such as open, restricted, or closed |
| `license` | License metadata returned by Zenodo |
| `creators` | Creator metadata returned by Zenodo |
| `keywords` | Keywords returned by Zenodo |
| `version` | Record version, when provided |
| `communities` | Zenodo community metadata |
| `resource_count` | Number of file resources associated with the record |
| `usability_label` | Backend-generated label based on file formats |
| `api_status` | Backend-generated status, such as `ok` or `ok_no_files` |
| `query_source` | Indicates whether the data came from `keywords`, `q`, `record_id`, `doi`, or generic params |
| `raw_metadata` | JSON string containing selected original Zenodo metadata |
| `notes` | Reserved notes column, currently `None` |

Example:

```python
datasets_df = zenodo.get_table("datasets")
print(datasets_df[["dataset_id", "doi", "title", "resource_count"]])
```

With DSI:

```python
datasets_df = dsi.get_table("datasets", collection=True)
print(datasets_df[["dataset_id", "doi", "title", "resource_count"]])
```

---

## `resources` Table

The `resources` table contains one row per file attached to a Zenodo record.

The backend retrieves file metadata but does not automatically download files.

| Column | Description |
|--------|-------------|
| `resource_id` | Backend-generated resource ID using the pattern `{dataset_id}:{index}` |
| `dataset_id` | Parent Zenodo record ID |
| `source_repository` | Source repository name, always `Zenodo` |
| `dataset_title` | Parent record title |
| `name` | File name from Zenodo metadata |
| `download_url` | Zenodo API file URL |
| `format` | File extension inferred from file name or URL |
| `size` | File size in bytes, when available |
| `checksum` | File checksum returned by Zenodo |
| `mimetype` | File MIME type, when available |
| `resource_type` | Parent record resource type |
| `source` | Source field inside the Zenodo record, usually `zenodo.files[]` |
| `url_valid` | URL validation result; `None` until URL validation is run |
| `raw_metadata` | JSON string containing original Zenodo file metadata |

Example:

```python
resources_df = zenodo.get_table("resources")
print(resources_df[["resource_id", "name", "format", "size", "download_url"]])
```

With DSI:

```python
resources_df = dsi.get_table("resources", collection=True)
print(resources_df[["resource_id", "name", "format", "size", "download_url"]])
```

---

## Relationship Between Tables

```text
datasets.dataset_id
    |
    |
    V
resources.dataset_id
```

The `dataset_id` field in the `datasets` table matches the `dataset_id` field in the `resources` table.

This creates a one-to-many relationship:

- One Zenodo record appears once in `datasets`.
- Each attached file appears as one row in `resources`.
- A single Zenodo record may have zero, one, or many file resources.

Example:

```text
datasets
--------
16537543 | Example Zenodo Record

resources
---------
16537543:1 | file1.csv
16537543:2 | file2.json
16537543:3 | documentation.pdf
```

---

## Metadata

### Curated Metadata

Frequently used metadata fields are extracted into stable table columns for easy access:

- Filtering
- Searching
- Summarization
- Reporting
- Export
- Conversion to local databases

These curated fields appear directly in the `datasets` and `resources` tables.

### Full Metadata

Selected original Zenodo metadata is preserved as JSON strings in:

```python
raw_metadata
```

Both `datasets` and `resources` contain a `raw_metadata` column.

Example:

```python
datasets_df = zenodo.get_table("datasets")
full_record_metadata = datasets_df.iloc[0]["raw_metadata"]

resources_df = zenodo.get_table("resources")
full_file_metadata = resources_df.iloc[0]["raw_metadata"]
```

This ensures:

- Important original API metadata is preserved
- Metadata can be exported cleanly to CSV or SQLite
- DSI table values remain serializable
- Future workflows can inspect fields not exposed as first-class columns

---

## File Format Usability Labels

The backend assigns a `usability_label` to each dataset based on the file extensions found in its resources.

Possible labels include:

| Label | Meaning |
|-------|---------|
| `tabular_or_easy_parse` | Dataset has files such as CSV, TSV, Excel, JSON, XML, TXT, or Parquet |
| `scientific_structured` | Dataset has files such as NetCDF, HDF5, FITS, MAT, NPY, NPZ, Zarr, or DAT |
| `archive_only` | Dataset only has archive-style files such as ZIP, TAR, TAR.GZ, GZ, 7Z, or RAR |
| `document_only` | Dataset only has document-style files such as PDF, DOC, DOCX, PPT, or PPTX |
| `unknown_format` | No file extensions were detected |
| `other_format` | File extensions were present but did not match the above categories |

Example:

```python
datasets_df = zenodo.get_table("datasets")
print(datasets_df[["title", "resource_count", "usability_label"]])
```

---

## Common DSI Operations

### List Tables

```python
dsi.list()
```

Example output:

```text
datasets: (3 rows, 23 cols)
resources: (22 rows, 14 cols)
```

### Return Table Names

```python
table_names = dsi.list(collection=True)
print(table_names)
```

Example output:

```text
['datasets', 'resources']
```

### View Backend Summary

```python
dsi.summary()
```

### Retrieve Tables

```python
datasets_df = dsi.get_table("datasets", collection=True)
resources_df = dsi.get_table("resources", collection=True)
```

### Display Table Preview

```python
dsi.display("datasets", num_rows=5)
dsi.display("resources", num_rows=10)
```

Use `display_cols` to focus on specific columns:

```python
dsi.display(
    "datasets",
    num_rows=5,
    display_cols=["dataset_id", "doi", "title", "resource_count"]
)

dsi.display(
    "resources",
    num_rows=10,
    display_cols=["resource_id", "name", "format", "size", "download_url"]
)
```

### Search Loaded Metadata

```python
dsi.search("Zenodo")
```

`search()` searches across:

- Table names
- Column names
- Cell values

### Filter Data Directly Through the Backend

The Zenodo backend supports local pandas-style filtering through `query_artifacts()`.

```python
zenodo = Zenodo(
    params={"keywords": "climate", "limit": 5},
    verify_ssl=False
)

datasets_result = zenodo.query_artifacts(
    "resource_count >= 0",
    dict_return=False
)

resources_result = zenodo.query_artifacts(
    "size >= 0",
    dict_return=False
)
```

> **Note:** `query_artifacts()` filters already-loaded DSI tables. It is not SQL.

### Find Rows Directly Through the Backend

```python
matches = zenodo.find_relation("resource_count", ">= '1'")
```

String contains search:

```python
matches = zenodo.find_relation("title", "~ 'climate'")
```

Exact format search:

```python
matches = zenodo.find_relation("format", "= 'csv'")
```

Supported relation operators:

```text
=, ==, !=, >, <, >=, <=, ~, ~~
```

The `~` and `~~` operators perform case-insensitive substring matching.

---

## Working with Resources

Resource rows contain file-level metadata and download URLs. The backend does not automatically download files during metadata retrieval.

### Access Resource URLs

```python
resources_df = zenodo.get_table("resources")

for _, row in resources_df.iterrows():
    print(f"Name: {row['name']}")
    print(f"Format: {row['format']}")
    print(f"Size: {row['size']}")
    print(f"URL: {row['download_url']}")
    print()
```

### Filter Resources by Format

```python
resources_df = zenodo.get_table("resources")

csv_resources = resources_df[
    resources_df["format"].fillna("").str.lower() == "csv"
]

print(csv_resources[["name", "size", "download_url"]])
```

### Download a Resource Manually

```python
import requests

resources_df = zenodo.get_table("resources")

first_resource = resources_df.dropna(subset=["download_url"]).iloc[0]
url = first_resource["download_url"]
name = first_resource["name"] or "zenodo_resource"

response = requests.get(url, timeout=60)
response.raise_for_status()

with open(name, "wb") as output_file:
    output_file.write(response.content)

print(f"Downloaded: {name}")
```

> **Note:** Downloading files is intentionally separate from metadata retrieval. The backend is metadata-first and does not automatically download resources.

---

## Validate Resource URLs

The backend can check whether resource URLs are accessible.

```python
zenodo = Zenodo(
    params={"keywords": "climate", "limit": 3},
    verify_ssl=False
)

valid_list = zenodo.validate_urls()
print(valid_list)

resources_df = zenodo.get_table("resources")
print(resources_df[["name", "download_url", "url_valid"]])
```

The `url_valid` column is:

| Value | Meaning |
|-------|---------|
| `True` | URL responded successfully |
| `False` | URL did not respond successfully |
| `None` | URL validation was not run or could not be determined |

---

## Export Data

### Export Tables to CSV with Pandas

```python
datasets_df = zenodo.get_table("datasets")
resources_df = zenodo.get_table("resources")

datasets_df.to_csv("zenodo_datasets.csv", index=False)
resources_df.to_csv("zenodo_resources.csv", index=False)
```

### Export Through DSI Writer

```python
dsi.write(
    filename="zenodo_datasets.csv",
    writer_name="Csv",
    table_name="datasets"
)
```

```python
dsi.write(
    filename="zenodo_resources.csv",
    writer_name="Csv",
    table_name="resources"
)
```

---

## Process to Writable Backend

Convert read-only Zenodo metadata to a local SQLite database.

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="Zenodo",
    params={"keywords": "climate", "limit": 10},
    verify_ssl=False
)

dsi.process(
    backend_name="Sqlite",
    filename="zenodo_climate.db"
)

dsi.close()

local_dsi = DSI(
    backend_name="Sqlite",
    filename="zenodo_climate.db"
)

datasets_df = local_dsi.get_table("datasets", collection=True)
resources_df = local_dsi.get_table("resources", collection=True)

print(f"Loaded {len(datasets_df)} datasets")
print(f"Loaded {len(resources_df)} resources")

local_dsi.close()
```

---

## Example Scripts

The following example scripts demonstrate common workflows with the Zenodo backend. All scripts are in `examples/zenodo/`.

### 1. `1_basic_keyword_search.py`

Initialize the Zenodo backend with a simple keyword search and inspect the returned tables.

Demonstrates:

- Basic search with `keywords`
- `get_table_names()`
- `list()`
- `get_table("datasets")`
- `get_table("resources")`

---

### 2. `2_record_and_doi_lookup.py`

Load a specific Zenodo record using record ID and DOI.

Demonstrates:

- `record_id` lookup
- `doi` lookup
- Confirming both lookup styles return the same record
- Inspecting associated file resources

---

### 3. `3_tables_summary_schema.py`

Explore the DSI table structure returned by the backend.

Demonstrates:

- `list()`
- `num_tables()`
- `summary()`
- `display()`
- `get_schema()`
- Selecting specific display columns

---

### 4. `4_filter_and_find.py`

Filter already-loaded Zenodo tables and search metadata.

Demonstrates:

- `query_artifacts()` with pandas-style conditions
- `find_table()`
- `find_column()`
- `find_cell()`
- `find()`
- `find_relation()`

Supported relation operators:

```text
=, ==, !=, >, <, >=, <=, ~, ~~
```

---

### 5. `5_validate_urls_and_export.py`

Validate resource URLs and export metadata tables.

Demonstrates:

- `validate_urls()`
- Inspecting the `url_valid` column
- Exporting `datasets` to CSV
- Exporting `resources` to CSV

---

### 6. `6_dsi_wrapper_basic.py`

Use Zenodo through the user-facing `DSI` wrapper.

Demonstrates:

- `DSI(backend_name="Zenodo", ...)`
- `list()`
- `list(collection=True)`
- `num_tables()`
- `get_table()`
- `display()`
- `summary()`
- `search()`

---

## Notes

- The backend is **read-only**
- The backend is **metadata-first**
- The backend returns exactly two tables: `datasets` and `resources`
- The backend does not create an errors table
- The backend does not download files automatically
- File metadata is stored in the `resources` table
- Record metadata is stored in the `datasets` table
- Original metadata is preserved in `raw_metadata` as a JSON string
- `dataset_id` links `datasets` and `resources`
- A single Zenodo record can have multiple files
- A Zenodo record can also have no files
- Invalid record ID format raises `ValueError`
- Invalid DOI format raises `ValueError`
- Valid but unmatched record IDs or DOIs return empty tables

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'dsi.backends.zenodo'`

Make sure `zenodo.py` is located here:

```text
dsi/backends/zenodo.py
```

Run from the repository root:

```bash
PYTHONPATH=. python examples/zenodo/1_basic_keyword_search.py
```

or install the repository in editable mode:

```bash
pip install -e .
```

### SSL Certificate Errors

Some local environments may fail SSL verification with errors similar to:

```text
SSLCertVerificationError: certificate verify failed
```

For local testing only, use:

```python
Zenodo(
    params={"keywords": "climate", "limit": 3},
    verify_ssl=False
)
```

or:

```python
DSI(
    backend_name="Zenodo",
    params={"keywords": "climate", "limit": 3},
    verify_ssl=False
)
```

> **Note:** Production environments should use proper certificate configuration instead of disabling SSL verification.

### Empty Results

If your query returns no records:

```python
zenodo.list()
```

Example output:

```text
datasets: (0 rows, 23 cols)
resources: (0 rows, 14 cols)
```

Try:

- Broadening search terms
- Removing filters
- Increasing `limit`
- Checking DOI spelling
- Checking record ID spelling
- Testing the query directly in the Zenodo search interface

### `dsi.query()` Returns a Dict Error

If your current `core.py` cannot be changed, avoid `dsi.query()` for the Zenodo backend and use direct backend filtering instead:

```python
zenodo.query_artifacts("resource_count >= 0", dict_return=False)
```

The direct backend method returns a pandas DataFrame when one table matches.

---

## Performance Tips

- Use `limit` to control result size
- Start with broad keyword queries, then refine
- Use direct record ID or DOI lookup when you know the exact record
- Use `display_cols` in `display()` to focus on useful columns
- Use pandas DataFrame filtering after retrieval for complex local analysis
- Use `process()` to cache read-only Zenodo metadata into a local database for repeated analysis
- Avoid validating many resource URLs unless needed, because URL checks require extra network requests

---

## Minimal Complete Example

```python
from dsi.backends.zenodo import Zenodo

zenodo = Zenodo(
    params={"keywords": "climate", "limit": 3},
    verify_ssl=False
)

try:
    zenodo.list()

    datasets = zenodo.get_table("datasets")
    resources = zenodo.get_table("resources")

    print(datasets[["dataset_id", "doi", "title", "resource_count"]])
    print(resources[["resource_id", "name", "format", "size", "download_url"]])

finally:
    zenodo.close()
```
