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
<summary><b>API Reference</b></summary>

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

## Current Integration Status

The Zenodo backend currently works as a **direct backend**:

```python
from dsi.backends.zenodo import Zenodo
```

At this stage, Zenodo is **not yet exposed through the user-facing `DSI(backend_name="Zenodo")` wrapper** unless `dsi.py` and `core.py` are updated to include it.

Use the direct backend examples in:

```text
examples/backends/zenodo/
```

Do not use:

```python
DSI(backend_name="Zenodo", ...)
```

until Zenodo is officially added to the supported DSI backend list.

---

## Quick Start

### Initialize the Backend

```python
from dsi.backends.zenodo import Zenodo

zenodo = Zenodo(
    params={"keywords": "climate", "limit": 5},
    verify_ssl=False
)
```

### List Available Tables

```python
zenodo.list()
```

Example output:

```text
datasets: (3 rows, 23 cols)
resources: (22 rows, 14 cols)
```

To return table names as a Python list:

```python
table_names = zenodo.list(collection=True)
print(table_names)
```

Example output:

```text
['datasets', 'resources']
```

### Access Tables

```python
datasets_df = zenodo.get_table("datasets")
resources_df = zenodo.get_table("resources")

print(datasets_df)
print(resources_df)
```

### Close the Backend

```python
zenodo.close()
```

A context manager is also supported:

```python
from dsi.backends.zenodo import Zenodo

with Zenodo(
    params={"keywords": "climate", "limit": 5},
    verify_ssl=False
) as zenodo:
    zenodo.list()
    datasets = zenodo.get_table("datasets")
    resources = zenodo.get_table("resources")
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

Convenience constructor:

```python
zenodo = Zenodo(
    keywords="climate",
    limit=10,
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

The backend returns exactly two DSI tables:

1. **`datasets`** - Record-level Zenodo metadata, one row per Zenodo record.
2. **`resources`** - File-level Zenodo metadata, one row per file attached to a Zenodo record.

The backend does **not** create an errors table.

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

## Common Backend Operations

### List Tables

```python
zenodo.list()
```

Example output:

```text
datasets: (3 rows, 23 cols)
resources: (22 rows, 14 cols)
```

### Return Table Names

```python
table_names = zenodo.list(collection=True)
print(table_names)
```

Example output:

```text
['datasets', 'resources']
```

### Retrieve Tables

```python
datasets_df = zenodo.get_table("datasets")
resources_df = zenodo.get_table("resources")
```

### View Backend Summary

```python
summary = zenodo.summary()
print(summary)
```

Single-table summary:

```python
summary = zenodo.summary("datasets")
print(summary)
```

### Display Table Preview

```python
print(zenodo.display("datasets", num_rows=5))
print(zenodo.display("resources", num_rows=10))
```

Use `display_cols` to focus on specific columns:

```python
print(
    zenodo.display(
        "datasets",
        num_rows=5,
        display_cols=["dataset_id", "doi", "title", "resource_count"]
    )
)

print(
    zenodo.display(
        "resources",
        num_rows=10,
        display_cols=["resource_id", "name", "format", "size", "download_url"]
    )
)
```

### Get Schema

```python
schema = zenodo.get_schema()
print(schema)
```

Example output:

```text
CREATE TABLE datasets (
    dataset_id TEXT,
    concept_record_id TEXT,
    doi TEXT,
    ...
);

CREATE TABLE resources (
    resource_id TEXT,
    dataset_id TEXT,
    source_repository TEXT,
    ...
);
```

---

## Filtering Loaded Tables

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

print(datasets_result)
print(resources_result)
```

> **Note:** `query_artifacts()` filters already-loaded DSI tables. It is not SQL.

---

## Find Methods

The Zenodo backend supports RCSBPDB-style find methods.

### `find()`

`find()` combines:

- `find_table()`
- `find_column()`
- `find_cell()`

```python
matches = zenodo.find("doi")

for match in matches:
    print(match.to_dict())
```

### `find_table()`

Search input across table names.

```python
matches = zenodo.find_table("data")

for match in matches:
    print(match.to_dict())
```

### `find_column()`

Search input across column names.

```python
matches = zenodo.find_column("doi")

for match in matches:
    print(match.to_dict())
```

### `find_cell()`

Search input across cell values and return matching rows as `ValueObject` instances.

```python
matches = zenodo.find_cell("Zenodo")

for match in matches:
    print(match.to_dict())
```

### `find_relation()`

Filter rows using a column-level relation.

Split-argument style:

```python
matches = zenodo.find_relation("resource_count", ">= 1")
```

One-string condition style:

```python
matches = zenodo.find_relation("resource_count >= 1")
```

String contains search:

```python
matches = zenodo.find_relation("title", "~ climate")
```

Exact format search:

```python
matches = zenodo.find_relation("format", "= csv")
```

Supported relation operators:

```text
=, ==, !=, >, <, >=, <=, ~, ~~
```

The `~` and `~~` operators perform case-insensitive substring matching.

### API-Backed `find_relation()`

Some `find_relation()` calls perform a new Zenodo API lookup/search and return `self.tables`.

Record ID lookup:

```python
tables = zenodo.find_relation("record_id = 16537543")
```

Dataset ID lookup:

```python
tables = zenodo.find_relation("dataset_id = 16537543")
```

DOI lookup:

```python
tables = zenodo.find_relation("doi = 10.5281/zenodo.16537543")
```

Keyword search:

```python
tables = zenodo.find_relation("keywords ~ climate", limit=3)
```

Query string search:

```python
tables = zenodo.find_relation("q ~ battery", limit=3)
```

> **Note:** API-backed `find_relation()` calls reload the backend tables.

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

> **Note:** `validate_urls()` may make extra HTTP HEAD/GET requests because it checks each resource URL.

---

## Export Data

### Export Tables to CSV with Pandas

```python
datasets_df = zenodo.get_table("datasets")
resources_df = zenodo.get_table("resources")

datasets_df.to_csv("zenodo_datasets.csv", index=False)
resources_df.to_csv("zenodo_resources.csv", index=False)
```

### Export Example Output Paths

The export example writes files to:

```text
examples/backends/zenodo/zenodo_datasets_export.csv
examples/backends/zenodo/zenodo_resources_export.csv
```

---

## Example Scripts

The following example scripts demonstrate common workflows with the Zenodo backend.

All scripts are in:

```text
examples/backends/zenodo/
```

### 1. `1.load_basic.py`

Initialize the Zenodo backend with a simple keyword search and inspect the returned tables.

Demonstrates:

- Basic search with `keywords`
- `list()`
- `list(collection=True)`
- `get_table("datasets")`
- `get_table("resources")`

Run:

```bash
PYTHONPATH=. python examples/backends/zenodo/1.load_basic.py
```

---

### 2. `2.lookup_record_doi.py`

Load a specific Zenodo record using record ID and DOI.

Demonstrates:

- `record_id` lookup
- `doi` lookup
- Inspecting associated file resources

Run:

```bash
PYTHONPATH=. python examples/backends/zenodo/2.lookup_record_and_doi.py
```

---

### 3. `3.tables_summary.py`

Explore the DSI table structure returned by the backend.

Demonstrates:

- `list(collection=True)`
- `list()`
- `num_tables()`
- `summary()`
- `display()`
- `get_schema()`
- Selecting specific display columns

Run:

```bash
PYTHONPATH=. python examples/backends/zenodo/3.list_tables.py
```

---

### 4. `4.filter_find.py`

Filter already-loaded Zenodo tables and search metadata.

Demonstrates:

- `query_artifacts()` with pandas-style conditions
- `find_table()`
- `find_column()`
- `find_cell()`
- `find()`
- `find_relation()` with split arguments
- `find_relation()` with one-string conditions
- API-backed `find_relation()` lookup/search

Run:

```bash
PYTHONPATH=. python examples/backends/zenodo/4.filter_and_find.py
```

---

### 5. `5.validate_urls_export.py`

Validate resource URLs and export metadata tables.

Demonstrates:

- `validate_urls()`
- Inspecting the `url_valid` column
- Exporting `datasets` to CSV
- Exporting `resources` to CSV

Run:

```bash
PYTHONPATH=. python examples/backends/zenodo/5.validate_urls_and_export.py
```

---

## Run All Examples

From the repository root:

```bash
for f in examples/backends/zenodo/*.py; do
    echo "Running $f"
    PYTHONPATH=. python "$f"
done
```

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
- `find_cell()` returns matching rows as `ValueObject` instances
- API-backed `find_relation()` calls reload backend tables and return `self.tables`
- `get_table_names()` is not used by the examples; use `list(collection=True)` instead

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'dsi.backends.zenodo'`

Make sure `zenodo.py` is located here:

```text
dsi/backends/zenodo.py
```

Run examples from the repository root:

```bash
PYTHONPATH=. python examples/backends/zenodo/1.load_basic.py
```

or install the repository in editable mode:

```bash
pip install -e .
```

### `DSI(backend_name="Zenodo")` Is Not Supported

If you see:

```text
RuntimeError: Please check the 'backend_name' argument as it is not supported by DSI
Eligible backend_names are: Sqlite, DuckDB, NDP, OSTI, Oceans11, RCSBPDB
```

then Zenodo has not yet been added to the DSI wrapper backend list.

Use the backend directly:

```python
from dsi.backends.zenodo import Zenodo

zenodo = Zenodo(
    params={"keywords": "climate", "limit": 3},
    verify_ssl=False
)
```

Do not use:

```python
DSI(backend_name="Zenodo", ...)
```

until `dsi.py` and `core.py` support Zenodo.

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

### `query_artifacts()` Returns No Results

If a local filter does not match any loaded rows, `query_artifacts()` may raise:

```text
ValueError: Query returned no results
```

Example:

```python
zenodo.query_artifacts("resource_count >= 0", dict_return=False)
```

Make sure the column exists in one of the loaded tables and that the condition matches at least one row.

### `find_relation()` Reloaded My Tables

Some `find_relation()` calls are API-backed and intentionally reload the backend tables.

These include:

```python
zenodo.find_relation("record_id = 16537543")
zenodo.find_relation("dataset_id = 16537543")
zenodo.find_relation("doi = 10.5281/zenodo.16537543")
zenodo.find_relation("keywords ~ climate", limit=3)
zenodo.find_relation("q ~ battery", limit=3)
```

Run API-backed `find_relation()` calls after local filtering examples if you do not want the loaded data to change before filtering.

---

## Performance Tips

- Use `limit` to control result size
- Start with broad keyword queries, then refine
- Use direct record ID or DOI lookup when you know the exact record
- Use `display_cols` in `display()` to focus on useful columns
- Use pandas DataFrame filtering after retrieval for complex local analysis
- Avoid validating many resource URLs unless needed, because URL checks require extra network requests
- Reuse a single backend instance for multiple local inspections instead of creating many new connections
- Close backend instances when done

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
