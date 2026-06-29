## NDP Backend for DSI

A read-only backend for accessing **NDP (CKAN-based) catalogs** via API and exposing metadata as DSI-compatible tables: `datasets` and `resources`.

**Note:** This backend is **read-only** — it only queries remote NDP instances.

---

## Quick Start

```python
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Fetch metadata
backend.query_artifacts(None, {"keywords": "energy", "limit": 10})

# Access tables
tables = backend.process_artifacts()
print(tables["datasets"].keys())
```

---

## Features

* **Fetch & transform metadata**

  * Query datasets/resources from NDP
  * Filters: `keywords`, `organization`, `tags`, `formats`, `limit`
  * Returns column-oriented `OrderedDict` tables

* **In-memory querying**

  ```python
  backend.query_in_memory("num_resources > 5", {"table": "datasets"})
  ```

* **Search utilities**

  * `find_table()`, `find_column()`, `find_cell()`

* **URL validation**

  * `validate_urls()` adds a `url_valid` column to `resources`

* **Inspection**

  * `notebook()` preview
  * `inspect_artifacts()` metadata summary

* **Lifecycle**

  * `query_artifacts()` **overwrites cache**
  * `close()` resets state

---

## Examples

Run the NDP examples:

1. `examples/ndp/1.ingest.py`
2. `examples/ndp/2.process.py`
3. `examples/ndp/3.query.py`
4. `examples/ndp/4.find.py`
5. `examples/ndp/5.inspect.py`
6. `examples/ndp/6.validate.py`
7. `examples/ndp/7.notebook.py`
8. `examples/ndp/8.close.py`
9. `examples/ndp/run_all.py`

---

## Notes

* `query_artifacts()` replaces previously loaded data (no append)
* `organization` is typically more reliable than `author` in CKAN metadata
* URL validation may depend on server behavior (some endpoints block requests)