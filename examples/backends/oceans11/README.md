# Oceans11 Backend for DSI

The Oceans11 backend is a read-only DSI backend for accessing datasets available through the LANL Oceans11 data catalog.

Oceans11 uses a two-tier data model:

* **Tier 1** contains catalog-level dataset metadata in the `records` table and associated file metadata in the `filesystem` table.
* **Tier 2** contains dataset-specific SQLite databases referenced by Tier 1 records. When a Tier 2 database is selected, its tables are downloaded and exposed through DSI using prefixed table names.

The backend downloads the Oceans11 catalog locally, searches the catalog using the supplied parameters, and loads matching Tier 1 and Tier 2 data into memory.

> **Note:** The Oceans11 backend is read-only. Data retrieved from Oceans11 can be processed into another writable DSI backend, such as SQLite.

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="Oceans11",
    params={"q": "heat", "rows": 5},
    workspace="./oceans11_data/",
)
```

The `workspace` directory is used for downloaded Oceans11 catalog and Tier 2 database files.

### List Available Tables

```python
dsi.list()
```

### Access the Tier 1 Records

```python
records = dsi.get_table("records", collection=True)
print(records)
```

### View a Summary

```python
dsi.summary()
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

Oceans11 searches are supplied through the `params` argument when creating the backend.

| Parameter       | Description                                                      |
| --------------- | ---------------------------------------------------------------- |
| `q`             | General keyword search across common metadata fields             |
| `keyword`       | Alias for `q`                                                    |
| `osti_id`       | Exact OSTI identifier                                            |
| `title`         | Partial title match                                              |
| `author`        | Partial author match                                             |
| `authors`       | Partial author match                                             |
| `subject`       | Partial subject match                                            |
| `subjects`      | Partial subject match                                            |
| `doi`           | Exact DOI                                                        |
| `report_number` | Exact report number                                              |
| `rows`          | Maximum number of Tier 1 records returned                        |
| `download_all`  | Load all Oceans11 Tier 1 records and associated Tier 2 databases |

### Keyword Search

```python
dsi = DSI(
    backend_name="Oceans11",
    params={"q": "heat", "rows": 5},
    workspace="./heat/",
)
```

### Title Search

```python
dsi = DSI(
    backend_name="Oceans11",
    params={"title": "monopoly", "rows": 10},
    workspace="./title/",
)
```

### Report Number Search

```python
dsi = DSI(
    backend_name="Oceans11",
    params={"report_number": "LA-UR-21-30892", "rows": 5},
    workspace="./recordID/",
)
```

### Multiple Queries

A list of parameter dictionaries can be supplied to combine multiple independent searches. Results are merged and duplicate Tier 1 records are removed.

```python
dsi = DSI(
    backend_name="Oceans11",
    params=[
        {"q": "heat", "rows": 5},
        {"title": "monopoly", "rows": 5},
    ],
    workspace="./multiple_queries/",
)
```

### Download All Records

```python
dsi = DSI(
    backend_name="Oceans11",
    params={"download_all": True},
    workspace="./download_all/",
)
```

This loads all Tier 1 records and downloads the Tier 2 databases referenced by those records.

---

## Tables

The tables available from the backend depend on the records selected by the initial search.

### Tier 1

Two catalog-level tables are loaded:

#### `records`

Contains dataset metadata such as:

* `osti_id`
* `title`
* `doi`
* `publication_date`
* `authors`
* `subjects`
* `report_number`
* `citation_url`
* `fulltext_url`
* `t2db_url`
* `t2db_path`
* `t2db_name`

The `t2db_path` field identifies the locally downloaded Tier 2 database associated with a record.

#### `filesystem`

Contains filesystem metadata associated with the selected Tier 2 databases, such as:

* file origin
* file size
* timestamps
* file UUID
* remote file location

### Tier 2

Each selected Tier 1 record may reference a separate Tier 2 SQLite database.

Tier 2 tables are loaded into the Oceans11 backend with the dataset/database name added as a prefix.

For example:

```text
heatequations_path_components
heatequations_files
heatequations_constants
heat_files
```

The exact Tier 2 table names depend on the datasets returned by the search.

This naming scheme prevents tables from different Tier 2 databases from overwriting one another.

---

## Common DSI Operations

### List Loaded Tables

```python
dsi.list()
```

To retrieve the table names as a collection:

```python
tables = dsi.list(collection=True)
print(tables)
```

### Retrieve a Table

```python
records = dsi.get_table("records", collection=True)
```

Tier 2 tables can be retrieved in the same way:

```python
constants = dsi.get_table(
    "heatequations_constants",
    collection=True,
)
```

### Display Data

```python
dsi.display("records", num_rows=5)
```

Specific columns can also be selected:

```python
dsi.display(
    "records",
    num_rows=5,
    display_cols=["osti_id", "title", "publication_date"],
)
```

### Search Loaded Data

Search across the currently loaded Oceans11 tables:

```python
dsi.search("97 MATHEMATICS AND COMPUTING")
```

`search()` searches table names, column names, and cell values. Matching cells are returned as complete rows when performing a DSI search.

### Find Relationships

Relations can be used to filter columns in loaded tables:

```python
dsi.find("osti_id == 2571471")
```


### View Summary Information

```python
dsi.summary()
```

A summary can also be generated for a specific table:

```python
dsi.summary("records")
```

### View the Schema

```python
print(dsi.schema())
```

---

## Process Oceans11 Data to SQLite

Because Oceans11 is read-only, loaded data can be processed into a writable SQLite database for local analysis.

```python
dsi = DSI(
    backend_name="Oceans11",
    params={"q": "heat", "rows": 5},
    workspace="./heat/",
)

dsi.process(
    "sqlite",
    "./heat/heat_search.db",
)

dsi.close()
```

All tables currently loaded by the Oceans11 backend are passed to the destination backend, including Tier 1 and loaded Tier 2 tables.

The resulting SQLite database can then be opened normally with DSI:

```python
local_dsi = DSI("./heat/heat_search.db")

local_dsi.list()

df = local_dsi.query(
    "SELECT * FROM records",
    collection=True,
)

local_dsi.close()
```

---

## Example Scripts

The scripts in `examples/oceans11/` demonstrate common Oceans11 workflows.

### 1. `1.keyword.py`

Performs a Tier 1 keyword search for Oceans11 records.

The example:

* Creates a workspace
* Searches using the `q` parameter
* Limits the number of returned records
* Downloads associated Tier 2 databases
* Processes the loaded data into SQLite
* Lists the loaded tables
* Retrieves selected metadata from `records`

Run with:

```bash
python examples/oceans11/1.keyword.py
```

---

### 2. `2.search_title.py`

Searches Oceans11 records using the `title` field.

The example:

* Creates a workspace
* Searches for records with a matching title
* Limits the result count with `rows`
* Downloads associated Tier 2 data
* Processes the results into SQLite
* Displays summary information

Run with:

```bash
python examples/oceans11/2.search_title.py
```

---

### 3. `3.download_all.py`

Downloads the complete Oceans11 catalog and all associated Tier 2 databases.

The example:

* Creates a clean workspace
* Initializes Oceans11 with `download_all=True`
* Loads all Tier 1 records
* Downloads referenced Tier 2 databases
* Loads Tier 2 tables into DSI
* Processes all loaded tables into a local SQLite database
* Displays summary information for the resulting data

Run with:

```bash
python examples/oceans11/3.download_all.py
```

> **Note:** This example can download substantially more data than the targeted search examples. Use an appropriate workspace with sufficient available storage.

---

### 4. `4.lookup_and_download.py`

Demonstrates the complete Oceans11 search-to-file workflow.

The example:

1. Searches Tier 1 metadata using a report number.
2. Processes the selected Oceans11 metadata into SQLite.
3. Retrieves the local Tier 2 database path from the `records` table.
4. Opens the Tier 2 database with DSI.
5. Queries the Tier 2 `files` table for selected files.
6. Downloads the referenced files into a local directory.

This demonstrates the relationship between:

```text
Oceans11 Tier 1
      |
      v
records.t2db_path
      |
      v
Tier 2 SQLite database
      |
      v
files.url
      |
      v
downloaded data files
```

Run with:

```bash
python examples/oceans11/4.lookup_and_download.py
```

---

## Oceans11 Data Workflow

A typical Oceans11 workflow is:

```text
Search Oceans11
      |
      v
Tier 1 records + filesystem
      |
      v
Download referenced Tier 2 database
      |
      v
Load Tier 2 tables into DSI
      |
      +----------------------+
      |                      |
      v                      v
Search / inspect        Process to SQLite
Tier 2 metadata
      |
      v
Locate file URLs
      |
      v
Download selected files
```

This allows users to search high-level dataset metadata before retrieving or working with dataset-specific information.

---

## Notes

* Oceans11 is a **read-only** DSI backend.
* The Oceans11 catalog is downloaded to the specified `workspace`.
* Tier 1 metadata is exposed through `records` and `filesystem`.
* Tier 2 databases are downloaded only for the records selected by the search, unless `download_all=True` is used.
* Tier 2 table names are prefixed with a human-readable dataset/database name.
* The number and names of Tier 2 tables vary between datasets.
* `process()` can be used to save all currently loaded Oceans11 tables to a writable backend such as SQLite.
* Actual dataset files referenced by Tier 2 metadata are not automatically downloaded by ordinary metadata searches. The `4.lookup_and_download.py` example demonstrates selective file retrieval.
* Use `dsi.close()` when finished with the backend.
