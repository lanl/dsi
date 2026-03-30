## CKAN Backend for DSI

A read-only backend to access CKAN catalogs via API and expose metadata as DSI-compatible tables (`datasets` and `resources`).

**Note:** This backend does **not** write data — it only queries remote CKAN instances.

### Quick Start

1. Import and initialize the backend:

```python
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "CKAN", "back-read")
backend = t.active_modules["back-read"][0]
```

2. Ingest metadata from a CKAN instance:

```python
backend.ingest_artifacts(None, {"keywords": "energy", "limit": 10})
```

3. Access processed artifacts:

```python
tables = backend.process_artifacts()
print(tables["datasets"].keys())
```

### Features

The CKAN backend provides multiple ways to inspect and query CKAN data:

* **Ingest and process metadata**

  * Fetch datasets and resources from a CKAN instance.
  * Apply filters: keywords, organization, tags, formats, limit.
  * Transform CKAN JSON into DSI-compatible tables (column-oriented `OrderedDict`).

* **Query artifacts**

  * Use `pandas.query()` on `datasets` or `resources` tables.
  * Example: fetch datasets with more than 10 resources:

    ```python
    result = backend.query_artifacts("`num_resources` > 10", {"table": "datasets"})
    ```

* **Search tables, columns, and cells**

  * `find_table(query)`, `find_column(query)`, `find_cell(query)` return matching results as `ValueObject`s.
  * Supports string or regex matching in table names, columns, and cell values.

* **URL validation**

  * Check all resource URLs in the `resources` table.
  * Adds a `url_valid` column indicating reachability.

* **Notebook / inspection**

  * Preview loaded datasets and resources:

    ```python
    backend.notebook()
    ```

* **Lifecycle management**

  * `close()` resets internal state.
  * `inspect_artifacts()` returns metadata about loaded tables.

### Examples

Run the following scripts to see backend usage in action:

1. **Ingest artifacts** – `examples/ckan/1.ingest.py`
2. **Process artifacts** – `examples/ckan/2.process.py`
3. **Query artifacts** – `examples/ckan/3.query.py`
4. **Find tables/columns/cells** – `examples/ckan/4.find.py`
5. **Inspect artifacts** – `examples/ckan/5.inspect.py`
6. **Run all examples** – `examples/ckan/run_all.py`