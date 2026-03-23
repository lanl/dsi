
# CKAN Backend Audit Report

## Overview

This report validates the CKAN backend integration within DSI.

It confirms:
- Backend loads correctly
- CKAN API connection works
- Data ingestion functions
- Query system works
- Find functions operate correctly
- Backend remains read-only

---

## Test Descriptions

### 1. test_ckan_backend_load
Ensures CKAN backend loads successfully inside DSI.

### 2. test_ckan_ingest
Verifies:
- CKAN API connectivity
- Metadata retrieval
- Dataset/resource caching

### 3. test_ckan_data_retrieved
Confirms that real data is returned from the CKAN catalog.

### 4. test_ckan_query
Tests backend query functionality on cached metadata.

### 5. test_ckan_find
Validates search capability (table, column, cell matching).

---



## Execution Results

Generated: 2026-03-23 12:48:23.783560

```
============================= test session starts ==============================
platform darwin -- Python 3.13.5, pytest-9.0.2, pluggy-1.6.0 -- /Users/kayahom/Desktop/Code/.ndp_venv/bin/python3.13
cachedir: .pytest_cache
rootdir: /Users/kayahom/Desktop/Code/DSI
configfile: pyproject.toml
collecting ... collected 5 items

examples/ckan_backend/test_ckan_backend.py::test_ckan_backend_load PASSED [ 20%]
examples/ckan_backend/test_ckan_backend.py::test_ckan_ingest PASSED      [ 40%]
examples/ckan_backend/test_ckan_backend.py::test_ckan_data_retrieved PASSED [ 60%]
examples/ckan_backend/test_ckan_backend.py::test_ckan_query PASSED       [ 80%]
examples/ckan_backend/test_ckan_backend.py::test_ckan_find PASSED        [100%]

=============================== warnings summary ===============================
examples/ckan_backend/test_ckan_backend.py::test_ckan_ingest
examples/ckan_backend/test_ckan_backend.py::test_ckan_data_retrieved
examples/ckan_backend/test_ckan_backend.py::test_ckan_query
examples/ckan_backend/test_ckan_backend.py::test_ckan_find
  /Users/kayahom/Desktop/Code/.ndp_venv/lib/python3.13/site-packages/urllib3/connectionpool.py:1097: InsecureRequestWarning: Unverified HTTPS request is being made to host 'nationaldataplatform.org'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
    warnings.warn(

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
======================== 5 passed, 4 warnings in 4.32s =========================


```
