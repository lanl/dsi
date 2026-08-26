# NAVDAT Backend for DSI

A read-only backend for accessing NAVDAT and federated geochemistry data via the live PetDB v4 API. Metadata is retrieved and exposed as DSI-compatible tables: `samples` and `citations`.

NAVDAT (navdat.org) was a web-accessible repository for age, chemical, and isotopic data from igneous rocks in western North America. **navdat.org itself has been static since 2014 and has no API.** Its data is now federated into PetDB 2.0 / EarthChem Synthesis alongside PetDB, GEOROC, SedDB, USGS, MetPetDB, and GANSEKI, and served live through the PetDB v4 API. This backend queries that live API rather than navdat.org directly.

Useful resources:

- [EarthChem / PetDB 2.0](https://earthchem.org/petdb): The current search interface for the federated data this backend queries.
- [PetDB API docs](https://earthchem.github.io/ec-doc/documentation/petdb_api.html): Documented endpoint/parameter reference (see "Known Limitations" below for what's confirmed vs. documented-only).
- [NAVDAT (legacy, static since 2014)](https://www.navdat.org/): Original site; data now lives in the federated system above.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify remote data.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses the live PetDB v4 API:

- Base URL: `https://api.earthchem.org`
- Connection check: `GET /v4/metrics`
- Samples: `GET /v4/locations/samples`
- Citations: `GET /v4/citations`
- Response envelope: `{"status": "success", "data": ...}`

An earlier version of this backend targeted the legacy EarthChem Portal REST Search Service (`portal.earthchem.org/restsearchservice`), based on archived documentation. Live verification (2026-08-24) found that service now returns `404 Not Found` — it appears to have been decommissioned as part of the PetDB 2.0 migration (the original PetDB Search was announced deprecated as of Dec 31, 2025). See `navdat.py`'s module docstring for the full diagnosis trail.

</details>

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="NAVDAT",
    params={"authors": "Walker", "size": 10}
)
```

### List Available Tables

```python
dsi.list()
```

### Access a Table

```python
samples_df = dsi.get_table("samples", collection=True)
print(samples_df)
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

**Confirmed working (live-tested, 2026-08-24):**

| Parameter | Type | Applies to | Notes |
|-----------|------|------------|-------|
| `authors` | str | `samples` AND `citations` | Confirmed to filter both: totalCount for `/v4/locations/samples` drops from 149,778 (unfiltered) to 1,558 with `authors=Walker` |
| `sampleNames` | str | `samples` | Literal sample name, NOT a rock-type keyword. `sampleNames=basalt` returns 0 rows — sample names look like `"(2.151) 12.40-12.85"`, not rock types |
| `size` | int | `samples` only | Page size; single page only (see Known Limitations) |

**Documented, passed through, but NOT yet confirmed live** — use with caution, and verify with a `curl` before relying on them:

`citationTitles`, `journals`, `publicationYears`, `laboratories`, `dataSources`, `expeditions`, `analysisTypes`, `geoFeatures`, `taxons`, `variables`, `boundingBox`, `polygons`, `precision`

The last four location/advanced filters use a documented `group::[value]` syntax (e.g. `taxons=igneous::[basalt]`) that has not been live-verified at all — this is the most likely place to find working rock-type filtering, but treat it as unproven until tested.

### Example: Author Search

```python
dsi = DSI(
    backend_name="NAVDAT",
    params={"authors": "Hekinian"}
)
```

### Example: Sample Name Lookup

```python
dsi = DSI(
    backend_name="NAVDAT",
    params={"sampleNames": "(2.151) 12.40-12.85"}
)
```

### Multiple Independent Queries

Like other DSI webserver backends, `params` can be a list of dicts (OR logic, deduplicated):

```python
dsi = DSI(
    backend_name="NAVDAT",
    params=[
        {"authors": "Walker", "size": 5},
        {"authors": "Hekinian", "size": 5},
    ]
)
```

---

## Tables

The backend returns up to two DSI tables:

1. **samples** — flattened sample/location metadata
2. **citations** — flattened citation/dataset/method metadata (only populated when the query returns citation matches)

**Important: `samples` and `citations` are NOT linked to each other.** No shared key was found in either endpoint's confirmed response shape (unlike, e.g., NDP's clean `datasets.id` → `resources.dataset_id` relationship). You cannot currently join "which samples came from which publication" through this backend. See Known Limitations.

---

### samples Table

One row per individual sample, flattened from `/v4/locations/samples`'s grouped/clustered response (each location group can contain multiple nested samples).

| Column | Description |
|--------|-------------|
| rootParent | Name of the location cluster/group this sample belongs to |
| groupDocCount | Document count for the whole location group |
| sampleCount | Number of samples in this group |
| groupLat / groupLon | Group-level coordinates |
| sampleName | Individual sample name |
| sampleId | Unique sample identifier |
| sampleDocCount | Document count for this specific sample |
| sampleLon / sampleLat | Individual sample coordinates |

**Example:**
```python
samples_df = dsi.get_table("samples", collection=True)
print(samples_df[["sampleName", "sampleLat", "sampleLon"]])
```

---

### citations Table

One row per citation/dataset record, flattened from `/v4/citations`. Nested arrays (`citationAuthors`, `methods`, `citationIdentifiers`) are flattened into joined-string columns.

| Column | Description |
|--------|-------------|
| citationId | Unique citation identifier |
| citationTitle | Citation article title |
| citationCode | Short citation code (e.g. `HEKINIAN, 1987`) |
| citationContainerTitle | Journal name |
| citationPublicationYear | Publication year |
| citationVolume / citationIssue / citationPages | Publication details |
| citationAuthors | Semicolon-joined author full names |
| citationDOIs | Comma-joined DOIs (from `citationIdentifiers` where `identifierType == "DOI"`) |
| datasetCode | Dataset code within the citation |
| datasetTitle | Dataset title |
| datasetNum | Dataset number |
| analysisType | e.g. `"Rock Analysis"`, `"Mineral Analysis"` |
| methods | Comma-joined analytical method names |

**Example:**
```python
citations_df = dsi.get_table("citations", collection=True)
print(citations_df[["citationTitle", "citationContainerTitle", "citationPublicationYear"]])
```

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

### View Table Schema
```python
print(dsi.schema("samples"))
```

### Retrieve a Table
```python
samples_df = dsi.get_table("samples", collection=True)
citations_df = dsi.get_table("citations", collection=True)
```

### Filter Data
```python
dsi.find("citationPublicationYear > 1990")
dsi.find("citationContainerTitle ~~ 'PETROL'")
```
Supports operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~` (contains)

### Search Loaded Metadata
```python
dsi.search("walker")
```

### Display Table Preview
```python
dsi.display("samples", num_rows=5)
dsi.display("citations", num_rows=5, display_cols=["citationTitle", "citationAuthors"])
```

### Process to Writable Backend
```python
dsi.process(backend_name="Sqlite", filename="navdat_local.db")
```

### Export Data
```python
dsi.write(filename="citations.csv", writer_name="Csv", table_name="citations")
```

---

## Example Scripts

All scripts are in `examples/navdat/`.

### 1. load_basic.py
Initialize the backend with an author search and view available tables.

### 2. display_tables.py
Preview samples and citations with different column selections.

### 3. find_basic.py
Filter citations by publication year, journal name, and analytical method using `find()`.

### 4. search_tables.py
Search across all loaded tables for a term with `search()`.

### 5. explore_metadata.py
Discover available journals, publication years, and analysis methods from loaded citation data. (Note: this works around a documented-but-unverified `/v4/authors` vocabulary endpoint that returned an empty body in testing — see Known Limitations.)

### 6. write_and_process.py
Export citations to CSV, and process the full loaded dataset into a local SQLite database for offline SQL querying.

---

## Known Limitations

Being explicit about these rather than letting the examples imply capabilities that don't exist:

- **No ID-based single-record lookup.** Unlike NDP's `id` param, there's currently no way to fetch one specific sample or citation directly by ID (`/v4/samples/:id` and `/v4/citations/:id` are documented but not wired into this backend yet).
- **No pagination beyond one page.** `/v4/locations/samples` accepts `size` (confirmed working) but no confirmed offset/page parameter has been tested. `/v4/citations` returns an `afterKey` cursor whose reuse as a request parameter is inferred from its shape, not confirmed live. Large queries will silently return only the first page.
- **`samples` and `citations` are not linked.** No shared key was found between them in the confirmed response shapes.
- **Most documented search props beyond `authors`/`sampleNames` are unverified.** In particular, rock-type/geochemistry-style filtering (`taxons`, `analysisTypes`, `geoFeatures`) — arguably the most important filter category for NAVDAT's actual domain — uses a documented `group::[value]` syntax that has never been tested against the live API.
- **`GET /v4/authors` returned an empty response body** in testing (2026-08-24) and wasn't investigated further; other vocabulary endpoints (`/v4/journals`, `/v4/laboratories`, etc.) are unverified.

---

## Troubleshooting

### Corporate TLS-inspecting proxies (e.g. Zscaler)

If `validate_connection()` fails with a generic connection/network error, but the same URL works fine in `curl` or a browser, this is very likely a masked SSL certificate verification error rather than an actual network problem — `requests` verifies against its own bundled `certifi` CA list, not the OS-native trust store, so it won't automatically trust a corporate proxy's intercepting root CA even when curl does (curl uses the OS trust store).

**Confirmed fix:** install [`truststore`](https://pypi.org/project/truststore/) (Python 3.10+) and inject it before any `requests` calls:
```python
import truststore
truststore.inject_into_ssl()
```
This is applied in `examples/navdat/` scripts' underlying test suite via `dsi/backends/tests/test_navdat.py`, scoped to test runs rather than hardcoded into `navdat.py` itself, since it changes SSL trust behavior for the whole process. See `navdat.py`'s module docstring for the full diagnosis trail — the first two things that looked like fixes (`REQUESTS_CA_BUNDLE`/`SSL_CERT_FILE` env vars) turned out not to work, confirmed via a clean isolated A/B test.

### Empty Results
```python
dsi.list()  # Check if tables exist at all
```
Try broadening or removing filters, or double-check `sampleNames` isn't being used with a rock-type keyword (it needs a literal sample name).
