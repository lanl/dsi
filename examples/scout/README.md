# DSIScout: Live Federation for DSI

`DSIScout` is a live federation layer that queries DSI's webserver backends (NDP, OSTI, Oceans11, RCSBPDB, Zenodo) and HPC-registered catalog chains together, with one shared set of search fields.

It's different from `DSIFederated` (`dsi/dsifederated.py`): `DSIFederated` queries databases that have already been downloaded to disk via `federate`/`pull_data`, using raw SQL. `DSIScout` performs live discovery against sources it's never seen before - it reconciles each source's different schema (field names, table names) through a crosswalk, and lets you search all of them with one call.

> **Note:** `DSIScout` is read-only - it pulls metadata (and, for HPC sources, lightweight catalog files), not full datasets.

---

## Quick Start

```python
from dsi.dsi_scout import DSIScout

scout = DSIScout(sources=["NDP", "Oceans11"], keyword="wildfire", limit=5)

scout.list()
scout.get_table("fed_results", collection=True)
scout.close()
```

Sources and query fields can also be given incrementally instead of at construction time:

```python
scout = DSIScout()
scout.add_source(webserver_name="NDP")
scout.add_source(webserver_name="OSTI")
scout.query(keyword="quic-fire", limit=5)
```

---

## Registering Sources

Two kinds of sources can be registered via `add_source()`:

**Webserver backends** - any of DSI's registered webserver backend names (`NDP`, `OSTI`, `Oceans11`, `RCSBPDB`, `Zenodo`):

```python
scout.add_source(webserver_name="NDP")
```

**HPC catalog sources** - a `location_type`/`location`/`path` pointing at either a CSV catalog (same schema as `federate_datasets.py`'s repo CSVs: `location_type, location, path, submitter_name, timestamp`) or a terminal database file:

```python
scout.add_source(
    location_type="url",
    location="oceans11.lanl.gov",
    path="https://oceans11.lanl.gov/dataCatalog/oceans11.db",
    submitter_name="example",
)
```

Supported `location_type` values: `github`, `hpc`, `hpc-kerberos`, `url`, `s3`, `local`.

Other registration methods:

```python
scout.list_sources()     # returns the list of registered source entry dicts
scout.delete_source(webserver_name="NDP")  # same arguments as add_source
```

`add_source`/`delete_source` raise `ValueError` on invalid input (unknown webserver name, duplicate source, incomplete HPC fields, no matching source to delete).

---

## Supported Query Fields

`query()` accepts a shared set of semantic fields:

| Field     | Description                                   |
| --------- | ---------------------------------------------- |
| `keyword` | General keyword search                        |
| `author`  | Author/creator search                          |
| `title`   | Title search                                   |
| `doi`     | DOI lookup                                     |
| `id`      | Source-specific identifier lookup              |
| `limit`   | Maximum number of rows per source              |

Not every source understands every field. Each webserver backend's actual query-parameter names are declared in `dsi/utils/webserver_crosswalk.json` (e.g. NDP's `keyword` maps to `keywords`, OSTI's maps to `q`). If a source doesn't support any of the given fields, it's skipped with a message rather than erroring. HPC sources' primary tables are filtered post-hoc using the matched schema's `field_map` in `dsi/utils/hpc_schema_crosswalk.json` (see "HPC Catalog Walking" below).

---

## Result Tables

`query()` produces:

- **`fed_results`** - one merged table (outer join on columns) combining every source's primary rows. Each row is tagged with `fed_source` (`"OSTI"`, `"NDP"`, or `"hpc:<location>"`) and a synthetic `fed_id` (`"<backend>:<original_pk>"` for webservers, `"hpc:<location>:<row index>"` for HPC sources).
- **Secondary tables** - every other table a source returns, named `<backend>_<table>` (webservers) or `hpc_<location>_<table>` (HPC). If a secondary table mixes rows from multiple parent records (e.g. NDP's `resources`), it's tagged with `fed_parent_id` pointing back to the owning `fed_id`.

Results are stored in `DSIScout`'s own internal backend and are also returned directly from `query()` as `{"fed_results": df, **secondary_tables}`.

---

## HPC Catalog Walking

An HPC source's `path` can point to:

1. **A CSV catalog** (same schema as `federate_datasets.py`'s repo CSVs) - each row is itself a pointer, recursively walked the same way, to another CSV or a terminal database.
2. **A terminal database file** - its tables are matched against the known schemas in `dsi/utils/hpc_schema_crosswalk.json` (each schema declares one expected table name + one required column). The first matching schema's primary table is filtered and merged into `fed_results`; every other table in that database loads as a secondary table. If no registered schema matches, a warning is printed and the walker falls back to a `resources`/`records` guess with no filtering.

`2.hpc_source.py` demonstrates this by registering the LANL Oceans11 catalog (`oceans11.lanl.gov/dataCatalog/oceans11.db`) directly as a `url` HPC source - **not** via the dedicated `Oceans11` webserver backend. The crosswalk's `oceans11` schema entry (`records` table + `t2db_url` column) is detected automatically, and the catalog's `records` rows merge into `fed_results` just like a webserver's primary table would. The walker does not follow Oceans11's own internal Tier-2 `t2db_url` pointers - that recursive tiered-pull behavior stays exclusive to the dedicated `Oceans11` webserver backend.

---

## Example Scripts

### 1. `1.basic_multi_webserver.py`

Registers NDP and OSTI, searches both for keyword `"quic-fire"` - LANL's QUIC-Fire wildfire-modeling software is catalogued on both NDP (its own software registration) and OSTI (its DOE research record), so this surfaces matching results from each source in one federated query.

```bash
python examples/scout/1.basic_multi_webserver.py
```

### 2. `2.hpc_source.py`

Registers the LANL Oceans11 catalog as a `url` HPC source and searches for keyword `"heat"`, demonstrating the HPC catalog-walk + schema-auto-detection path end to end, including the `filesystem` and `federated` secondary tables that come along with the Oceans11 Tier-1 catalog.

```bash
python examples/scout/2.hpc_source.py
```

### 3. `3.mixed_sources.py`

Registers NDP, OSTI, and the Oceans11 HPC source together at construction time (`DSIScout(sources=[...], keyword=...)`), and runs one `query()` across all three - the actual federation value proposition: one merged `fed_results` spanning two live webserver APIs and an HPC catalog chain.

```bash
python examples/scout/3.mixed_sources.py
```

---

## Using via the CLI

The same workflow is available through the DSI CLI's `scout_*` commands, which delegate to `DSIScout` internally:

```text
dsi> scout_add NDP
dsi> scout_add OSTI
dsi> scout_list
dsi> scout_fetch keyword:quic-fire limit:5
dsi> list
dsi> display fed_results
```

HPC sources use the same `--location-type`/`--location`/`--path` flags as `add_source()`:

```text
dsi> scout_add --location-type url --location oceans11.lanl.gov --path https://oceans11.lanl.gov/dataCatalog/oceans11.db --submitter-name example
dsi> scout_fetch keyword:heat limit:5
```

`scout_delete` takes the same arguments as `scout_add` to remove a registered source.

---

## Notes

- `DSIScout` is read-only with respect to the sources it federates - it does not ingest/modify them.
- Field support varies per source; see `dsi/utils/webserver_crosswalk.json` and `dsi/utils/hpc_schema_crosswalk.json` for exactly which fields each registered source/schema understands.
- No entity resolution/deduplication is performed across sources beyond what's visible in `fed_results`' columns (e.g. a shared `doi` or `osti_id` appearing in two sources' rows is not automatically merged into one row).
- HPC catalog chains have no cycle detection; a fixed recursion depth limit guards against runaway chains.
- Live webserver APIs can occasionally be slow or return transient errors; `query()` logs a per-source failure message and continues with whatever other sources succeeded, rather than aborting the whole query.
