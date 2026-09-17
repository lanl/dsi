# OSTI Backend for DSI

A read-only backend for accessing OSTI records via the REST API. Metadata is retrieved and exposed as a DSI-compatible table: `records`.

The U.S. Department of Energy's Office of Scientific and Technical Information (OSTI) catalog provides open access to DOE publications, openly released datasets, and software releases. The OSTI backend enables users to search and retrieve scientific and technical research metadata directly through DSI without downloading or managing the underlying dataset locally.

Useful OSTI resources:

- [OSTI.GOV](https://www.osti.gov/): Main catalog for searching, browsing, and accessing scientific and technical research results from the within OSTI.
- [OSTI Search](https://www.osti.gov/search): Browse and search the OSTI collection.
- [OSTI.GOV API Documentation](https://www.osti.gov/api/v1/docs): Official API reference for querying and retrieving OSTI record metadata.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote OSTI data.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses the OSTI.GOV REST API with the following endpoints:

- Base URL: `https://www.osti.gov/api/v1`
- API version: 1
- Search endpoint: `/records` — searches and filters OSTI records using supported query parameters.
- Single-record endpoint: `/records/{osti_id}` — retrieves a specific record directly by OSTI ID.

Query parameters are automatically formatted and filtered to the parameters supported by the backend.

</details>

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(backend_name="OSTI", params={"q": "wildfire", "rows": 10})
````

### List Available Tables

```python
dsi.list()
```

### Access a Table

```python
records_df = dsi.get_table("records", collection=True)
print(records_df)
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

The OSTI backend supports flexible record retrieval through the `params` argument. Search parameters correspond to fields supported by the OSTI.GOV `/records` endpoint.

### Keyword Search 

Use the `q` parameter to perform a general keyword search:

```python 
dsi = DSI(
    backend_name="OSTI",
    params={"q": "wildfire", "limit": 10}
)
```

### Title Search 

Use the `title` parameter to search for a term or phrase in the title: 

```python 
dsi = DSI( backend_name="OSTI", 
    params={ "title": "machine learning", "rows": 10 } 
)
```

### Author Search 

Use the `author` parameter to search OSTI records by author:
```python 
dsi = DSI( backend_name="OSTI",     
    params={ "author": "Linn", "rows": 10 } 
)
```

### OSTI ID 
Use the `osti_id` parameter to retrieve a record by its unique OSTI identifier:
```python 
dsi = DSI( backend_name="OSTI", 
    params={ "osti_id": "1234567" } 
)
```

### DOI Search 
Use the `doi` parameter to search for a record using its Digital Object Identifier (DOI):
```python 
dsi = DSI( backend_name="OSTI", 
    params={ "doi": "10.2172/1234567" }
)
```

### Combined Parameters

Multiple search parameters can be combined in a single request:

```python
dsi = DSI( backend_name="OSTI", 
    params={ "q": "wildfire", 
            "research_org": "Los Alamos National Laboratory", 
            "publication_date_start": "01/01/2020", 
            "has_fulltext": True, 
            "sort": "publication_date", 
            "order": "desc", 
            "rows": 25 } 
)
```

This allows a broad keyword search to be narrowed using additional metadata fields, date ranges, full-text availability, sorting, and pagination.

### Multiple Independent Queries

The OSTI backend also accepts a list of parameter dictionaries. Each dictionary is submitted as an independent OSTI request.

Results from all requests are combined into the `records` table and deduplicated using `osti_id` when available. If an OSTI ID is unavailable, the backend falls back to DOI and then title for deduplication.

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="OSTI",
    params=[
        {
            "q": "wildfire",
            "rows": 10
        },
        {
            "q": "climate modeling",
            "rows": 10
        },
        {
            "research_org": "Los Alamos National Laboratory",
            "subject": "machine learning",
            "rows": 10
        }
    ]
)
```

This is useful when several independent OSTI searches should be combined into a single DSI records table.

---

## Supported Parameters 

| Parameter | Type | Description | 
|-----------|------|-------------|
| `q` | `str` | General keyword search across OSTI records. | 
| `osti_id` | `str` or `int` | Unique OSTI identifier for a record. When supplied by itself, the backend uses the `/records/{osti_id}` endpoint. | 
| `doi` | `str` | Digital Object Identifier (DOI). | 
| `fulltext` | `str` | Searches available document full text. | 
| `biblio` | `str` | Searches bibliographic information associated with OSTI records. | 
| `author` | `str` | Searches records by author. | 
| `title` | `str` | Searches within record titles. | 
| `identifier` | `str` | Searches record identifiers, such as report or contract identifiers. | 
| `sponsor_org` | `str` | Searches by sponsoring organization. | 
| `research_org` | `str` | Searches by the organization responsible for conducting the research. | 
| `contributing_org` | `str` | Searches by contributing organization. | 
| `source_id` | `str` | Filters records using the OSTI source identifier. | 
| `publication_date_start` | `str` | Beginning publication-date boundary. Dates use `MM/DD/YYYY` format. | 
| `publication_date_end` | `str` | Ending publication-date boundary. Dates use `MM/DD/YYYY` format. | 
| `entry_date_start` | `str` | Beginning OSTI entry-date boundary. Dates use `MM/DD/YYYY` format. | 
| `entry_date_end` | `str` | Ending OSTI entry-date boundary. Dates use `MM/DD/YYYY` format. | 
| `language` | `str` | Filters records by language. | 
| `country` | `str` | Filters records by country of publication. | 
| `site_ownership_code` | `str` | Filters records by site ownership code. | 
| `subject` | `str` | Searches subjects associated with OSTI records. | 
| `has_fulltext` | `bool` | Filters records according to whether full text is available. | 
| `sort` | `str` | Specifies the field used to sort returned records. | 
| `order` | `str` | Specifies sort direction, typically `asc` or `desc`. | 
| `rows` | `int` | Number of records requested per page. The backend defaults to `20`. | 
| `page` | `int` | Results page to retrieve. The backend defaults to `1`. |

---

## Tables

The OSTI backend returns one DSI table:

1. **records** - OSTI record metadata (one row per OSTI record)

---

### records Table

The `records` table contains one row per OSTI record returned by the API. Frequently used metadata fields are extracted into individual columns, while the complete OSTI API response is preserved in `raw_record`.

Table fields include:

| Column | Description |
|--------|-------------|
| `osti_id` | Unique OSTI identifier for the record |
| `title` | Title of the publication, dataset, software release, or other research product |
| `doi` | Digital Object Identifier (DOI), when available |
| `publication_date` | Publication date associated with the record |
| `entry_date` | Date the record was entered or updated in OSTI |
| `language` | Language of the research product |
| `country_publication` | Country of publication |
| `product_type` | Type of research product represented by the record |
| `description` | Description or abstract associated with the record |
| `publisher` | Publisher of the research product |
| `journal_name` | Name of the journal, when applicable |
| `journal_volume` | Journal volume, when applicable |
| `journal_issue` | Journal issue, when applicable |
| `availability` | Availability information supplied by OSTI |
| `format` | Format information associated with the research product |
| `report_number` | Report number associated with the record |
| `doe_contract_number` | DOE contract number associated with the research |
| `nsa_number` | Nuclear Science Abstracts (NSA) number, when available |
| `authors` | Semicolon-separated list of record authors |
| `subjects` | Semicolon-separated list of subjects associated with the record |
| `sponsor_org` | Semicolon-separated list of sponsoring organizations |
| `research_org` | Semicolon-separated list of organizations responsible for the research |
| `contributor_org` | Semicolon-separated list of contributing organizations |
| `has_fulltext` | Boolean indicating whether a full-text URL is available |
| `citation_url` | OSTI citation URL, when available |
| `citation_doe_pages_url` | DOE PAGES citation URL, when available |
| `fulltext_url` | URL for the full-text research product, when available |
| `raw_record` | Complete OSTI API response for the record |

**Example:**

```python
records_df = dsi.get_table("records", collection=True)

print(
    records_df[
        [
            "osti_id",
            "title",
            "publication_date",
            "authors",
        ]
    ]
)
````

---

## Metadata

### Curated Metadata

Frequently used OSTI metadata fields are extracted into columns in the `records` table for easy access:

- Filtering
- Searching
- Summarization
- Reporting
- Exporting

These fields include commonly used record information such as:

- OSTI ID
- Title
- DOI
- Publication and entry dates
- Authors
- Subjects
- Research, sponsoring, and contributing organizations
- Journal information
- Report and contract numbers
- Full-text availability and URLs

These values appear directly as columns in the `records` table.

### Full Metadata

The complete OSTI API response for each record is preserved in:

```python
raw_record  # Full OSTI API response for the record
````

**Example:**

```python
records_df = dsi.get_table("records", collection=True)

full_metadata = records_df.iloc[0]["raw_record"]

print(full_metadata)
```

This ensures:

* No metadata loss
* Access to OSTI fields that are not extracted into dedicated columns
* Future compatibility if additional OSTI metadata fields are introduced
* Support for advanced workflows that require the original API response

```

---

## Common DSI Operations

### List Tables

```python
dsi.list()
````

### Get the Records Table

```python
records_df = dsi.get_table("records", collection=True)
```

### Display Records

```python
dsi.display("records")
```

Display a subset of columns:

```python
dsi.display(
    "records",
    display_cols=[
        "osti_id",
        "title",
        "publication_date",
        "authors"
    ]
)
```

### View Summary Information

```python
dsi.summary()
```

### Search Across Records

Use `search()` to find a value anywhere in the backend:

```python
dsi.search("Los Alamos")
```

Return matching data as a collection:

```python
results = dsi.search(
    "Los Alamos",
    collection=True
)
```

### Find Records by Condition

Use `find()` to locate records that satisfy a column-level condition:

Find records within a date range:

```python
dsi.find(
    "osti_id > 100000"
)
```

### Export Records

OSTI data can be exported using DSI writers.

```python
dsi.write(
    "osti_records.csv",
    "CSV",
    table_name="records"
)
```

### Save as SQLite DB

OSTI data can be exported using DSI writers.

```python
dsi.process(
    "SQLite",
    "records.db"
)
```

### Close the Backend

```python
dsi.close()
```

---

## Example Scripts

The following example scripts demonstrate common OSTI backend workflows. All scripts are located in `examples/osti/`.

### 1. `1.load_keywords.py`

Perform a basic keyword search against OSTI and process the returned metadata into a local SQLite database.

- Search OSTI using the general `q` parameter
- Limit the number of returned records with `rows`
- Process OSTI metadata into a SQLite database with `dsi.process()`
- List the loaded table
- Access the `records` table as a pandas DataFrame
- Display selected metadata fields including OSTI ID, title, and publication date

---

### 2. `2.search_title.py`

Search OSTI records using the `title` field.

- Search for records containing a specified title term or phrase
- Limit the number of returned records
- Access the resulting `records` table
- Display OSTI ID, title, and publication date
- Generate summary information for the loaded metadata

---

### 3. `3.lookup_osti_id.py`

Search OSTI records using a specific OSTI ID.

- Filter OSTI records with the `osti_id` parameter
- Retrieve matching record metadata
- Display OSTI ID, title, DOI, and publication date
- Generate summary information for the returned data

Because this example also supplies `rows`, the request is sent through the `/records` search endpoint using `osti_id` as a query parameter.

---

### 4. `4.fetch_reports.py`

Retrieve metadata for multiple report numbers in a single DSI workflow.

- Define a list of report numbers
- Build multiple independent OSTI queries using the `identifier` parameter
- Submit the queries as a list of parameter dictionaries
- Combine returned results into a single `records` table
- Display report number, OSTI ID, title, and publication date

This example demonstrates how the OSTI backend can combine multiple independent searches into one DSI table.

---

## Notes

- The OSTI backend is read-only and retrieves metadata from the OSTI.GOV REST API.
- Retrieved data is exposed through a single DSI table named `records`.
- Each row in `records` represents one OSTI record.
- Frequently used OSTI metadata fields are extracted into dedicated columns.
- The complete API response for each record is preserved in the `raw_record` column.
- The backend supports both single-query parameter dictionaries and lists of independent parameter dictionaries.
- Results from multiple queries are merged into the same `records` table and deduplicated using OSTI ID when available, with DOI and title used as fallbacks.
- When `osti_id` is the only supplied parameter, the backend retrieves the record through `/records/{osti_id}`. If additional parameters such as `rows` are supplied, the backend uses the `/records` search endpoint instead.
- OSTI metadata can be transferred to another DSI backend, such as SQLite, using `dsi.process()`.
- OSTI metadata can also be exported using DSI writers such as CSV.
- Loaded metadata can be explored with standard DSI operations including `list()`, `get_table()`, `display()`, `summary()`, `search()`, and `find()`.
- SQL-style `query()` operations are not supported directly by the OSTI backend.
- Full-text documents are not downloaded automatically. When available, their locations are stored in `fulltext_url`.
- `has_fulltext` indicates whether a full-text URL was provided for a record.

---

