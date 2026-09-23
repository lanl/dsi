# Denodo Backend for DSI

A read-only backend for accessing a Denodo Data Catalog via its REST API. Metadata is retrieved and exposed as a DSI-compatible table: `denodo_search_results`.

Denodo is a data virtualization platform: it exposes tables from many underlying systems as *views* inside *virtual databases*, and its Data Catalog adds descriptions, tags, categories and custom properties on top of them. This backend sends one metadata search to the Data Catalog, normalizes the response, and exposes it as a DSI table with one row per view returned.

Useful Denodo resources:

- [Denodo Data Catalog documentation](https://community.denodo.com/docs/html/browse/latest/en/vdp/data_catalog/index): Overview of the Data Catalog, its metadata model, and its search features.
- [Denodo Data Catalog REST API](https://community.denodo.com/docs/html/browse/latest/en/vdp/data_catalog/appendix/rest_api/rest_api): Official API reference.
- Your own Data Catalog instance: browse the same views in the web UI to see how a search result looks to a human.

> **Note:** This backend is read-only. It retrieves and organizes metadata but does not modify anything in Denodo.

> **Note:** A Denodo Data Catalog is usually an internal service protected by OAuth. Read **Configuration** before running anything.

<details>
<summary><b>API Reference (for developers)</b></summary>

The backend uses one Data Catalog endpoint:

- Base URL: taken from your configuration, never hard-coded (see **Configuration**)
- API prefix: `/denodo-data-catalog/public/api`
- Main endpoint: `POST /search/metadata`
- Connection check: `GET /home`

The request body is built from the `params` you pass. Fields that are fixed by the backend:

| Body field | Value | Why |
|---|---|---|
| `elementType` | `"VIEWS"` | the only element type this API serves for search |
| `withEndorsements`, `withWarnings`, `withDeprecations` | `False` | despite the names, these *filter* to only elements having that flag |
| `categoryIds`, `tagIds`, `databaseIds` | present, empty unless filtered | omitting any of them returns HTTP 400 |
| `offset`, `limit` | managed internally | pagination is automatic, and the total is checked against the server's own count |

No other endpoint is called: one search is one POST, plus one page of POSTs for large results.

</details>

---

## Configuration

Nothing site-specific is stored in this repository. Every setting resolves as
**argument → environment variable → `~/.denodo/config.json`**.

| Setting | Environment variable | Config-file key |
|---|---|---|
| base URL | `DENODO_BASE_URL` | `base_url` |
| authorize URL | `AUTH_URL` | `auth_url` |
| token URL | `TOKEN_URL` | `token_url` |
| client id | `AUTH_FLOW_CLIENT_ID` | `client_id` |
| client secret | `AUTH_FLOW_CLIENT_SECRET` | `client_secret` |
| redirect URI | `REDIRECT_URI` | `redirect_uri` |
| scope | `SCOPE` | `scope` |

Request the OAuth client settings from your Data Catalog administrator, then either set them as environment variables or store them once:

```python
from dsi.backends.denodo import save_config

save_config(base_url="https://<data-catalog-host>",
            client_id="...", client_secret="...",
            auth_url="...", token_url="...",
            redirect_uri="...", scope="...")
```

Check that a machine is ready:

```python
from dsi.backends.denodo import Denodo

print(Denodo(only_validate=True).validate_connection())   # True = configured and reachable
```

### Authentication

The backend uses the OAuth 2.0 authorization-code flow:

1. The first request opens your browser at the authorize URL.
2. After you log in, the identity provider redirects to the registered `redirect_uri` with a one-time code.
3. Paste that full redirect URL back at the prompt; the backend exchanges the code for an access token.

> **Note:** The redirect URI is registered with your identity provider and may point at a different host than the catalog you query.

To reuse a token you already have, pass it in and skip the browser entirely:

```python
dsi = DSI(backend_name="Denodo", token="<access token>", params={"keywords": "..."})
```

Tokens expire (typically after an hour). A backend created with a token keeps that token: build a new backend rather than expecting a refresh.

---

## Quick Start

### Initialize the Backend

```python
from dsi.dsi import DSI

dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"}
)
```

### List Available Tables

```python
dsi.list()
```

### Access the Table

```python
views_df = dsi.get_table("denodo_search_results", collection=True)
print(views_df)
```

### Close the Backend

```python
dsi.close()
```

---

## Supported Search Parameters

The backend supports flexible querying through a unified `params` interface.

### Keyword Search

Search for text. An empty string returns the whole catalog:

```python
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "waste"}
)
```

### Choosing Where to Search

`search_in` selects which part of a view is searched. The same word gives different results in each scope:

```python
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "area", "search_in": ["column_names"]}
)
```

| `search_in` value | Searches |
|---|---|
| `name` | the view name |
| `description` | the view's description |
| `properties` | the values of custom properties attached to the view |
| `column_names` | the names of the view's columns |
| `column_descriptions` | the descriptions of those columns |

> **Note:** Custom properties are searchable by **value** only, not by property name.

Scopes can be combined; a view matches if it matches in any of them:

```python
params={"keywords": "area", "search_in": ["name", "description"]}
```

### Choosing How to Match

`match` selects how the words in `keywords` are matched:

```python
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "area type", "search_in": ["name"], "match": "all_words"}
)
```

| `match` value | Rule |
|---|---|
| `substring` | the whole string must appear as one case-insensitive substring |
| `all_words` | every word must appear, in any order (AND) |
| `any_words` | any one word is enough (OR) |

> **Note:** `substring` is literal. View names often use underscores, so a phrase typed with spaces may match nothing while `all_words` finds it.

### Category and Tag Filters

Restrict a search to views carrying particular categories or tags, by id:

```python
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "", "categories": [80], "tags": [599]}
)
```

Ids come from the `categories` and `tags` columns of an earlier result, or from the Data Catalog UI.

### Limiting Results

Without `limit`, every matching view is retrieved, paginating internally:

```python
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "area", "limit": 25}
)
```

### Multiple Independent Queries

Run several queries and combine the results. Rows are deduplicated by `(database_name, name)`:

```python
dsi = DSI(
    backend_name="Denodo",
    params=[
        # Query 1: views whose name mentions area
        {"keywords": "area", "search_in": ["name"]},

        # Query 2: views carrying CUI in a custom property value
        {"keywords": "CUI", "search_in": ["properties"], "match": "substring"},

        # Query 3: views with a column about dates
        {"keywords": "date", "search_in": ["column_names"], "limit": 10}
    ]
)
```

---

## Supported Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `keywords` | str | `""` (whole catalog) | Text to search for |
| `search_in` | list[str] | `["name", "description"]` | Where to search: `name`, `description`, `properties`, `column_names`, `column_descriptions` |
| `match` | str | `"any_words"` | How to match: `substring`, `all_words`, `any_words` |
| `categories` | list[int] | no filter | Restrict to these category ids |
| `tags` | list[int] | no filter | Restrict to these tag ids |
| `limit` | int | every match | Maximum number of views to retrieve per query |

---

## Tables

The backend returns one DSI table:

1. **denodo_search_results** - View metadata from the search (one row per view returned)

Tables for databases, views, columns and custom properties, built from the Data Catalog's view-details endpoint, are planned.

---

### denodo_search_results Table:

The `denodo_search_results` table contains one row per view returned by the search. A view is identified by the pair `(database_name, name)`.

| Column | Description |
|--------|-------------|
| name | View name |
| database_name | Virtual database (VDB) the view belongs to |
| id | Numeric element id — a surrogate key, different in each environment |
| database_id | Numeric database id |
| description | The view's description, may be empty |
| descriptionType | Description format — always empty in search results |
| categories | Comma-separated list of category names |
| tags | Comma-separated list of tag names |
| lastModificationVdpData | Last modification on the Virtual DataPort side |
| lastModificationIsstData | Last modification on the catalog side |
| matchedFields | How many columns matched *this* query |
| matchedCustomProperties | How many custom properties matched *this* query |
| matchedFieldsTagged | How many tagged columns matched *this* query |
| countEndorsements | Number of endorsements on the view |
| countWarnings | Number of warnings on the view |
| countDeprecations | Number of deprecations on the view |
| ranking | Search ranking for *this* query |

**Example:**

```python
views_df = dsi.get_table("denodo_search_results", collection=True)
print(views_df[["name", "database_name", "categories", "tags"]])
```

---

## Metadata

### Curated Metadata

Every field the search endpoint returns becomes a column, so the table is a faithful image of the API response. Nested values are flattened so that each cell holds a single value:

| API response | Table columns |
|---|---|
| `database` (object) | `database_name`, `database_id` |
| `categories` (list of objects) | `categories` — names joined with `", "` |
| `tags` (list of objects) | `tags` — names joined with `", "` |

Empty lists become empty values rather than empty strings, so `dsi.find("tags ~~ 'x'")` and `WHERE tags LIKE '%x%'` behave predictably.

### Query-relative Columns

Four columns describe the **query**, not the view: `matchedFields`, `matchedCustomProperties`, `matchedFieldsTagged` and `ranking`. They change with the search and are often constant within one result set. They are kept because they are evidence of *why* a view matched, but they should not be compared across different queries.

---

## Common DSI Operations

### List Tables

```python
dsi.list()
```

**Output:**
```
Table: denodo_search_results
  - num of columns: 17
  - num of rows: 350
```

### View Backend Summary

```python
dsi.summary("denodo_search_results")
```

**Output** (illustrative — one row per column):
```text
Table: denodo_search_results

column                   | type   | unique | min                           | max                           | avg     | std_dev
-------------------------+--------+--------+-------------------------------+-------------------------------+---------+--------
name                     | OBJECT | 350    | area_type_ref                 | zone_workpath                 | nan     | nan
database_name            | OBJECT | 1      | sample_db                     | sample_db                     | nan     | nan
id                       | INT64  | 350    | 1000                          | 4200                          | 2480.51 | 545.80
database_id              | INT64  | 1      | 5                             | 5                             | 5.0     | 0.0
description              | OBJECT | 190    | None                          | None                          | nan     | nan
descriptionType          | OBJECT | 0      | None                          | None                          | nan     | nan
categories               | OBJECT | 6      | Facilities                    | Reference Data                | nan     | nan
tags                     | OBJECT | 98     | None                          | None                          | nan     | nan
lastModificationVdpData  | OBJECT | 300    | 2024-01-04T16:42:41.000+00:00 | 2025-03-04T20:01:42.000+00:00 | nan     | nan
lastModificationIsstData | OBJECT | 1      | 2025-09-09T14:55:55.000+00:00 | 2025-09-09T14:55:55.000+00:00 | nan     | nan
matchedFields            | INT64  | 1      | 0                             | 0                             | 0.0     | 0.0
matchedCustomProperties  | INT64  | 1      | 1                             | 1                             | 1.0     | 0.0
matchedFieldsTagged      | INT64  | 1      | 0                             | 0                             | 0.0     | 0.0
countEndorsements        | INT64  | 1      | 0                             | 0                             | 0.0     | 0.0
countWarnings            | INT64  | 1      | 0                             | 0                             | 0.0     | 0.0
countDeprecations        | INT64  | 1      | 0                             | 0                             | 0.0     | 0.0
ranking                  | INT64  | 1      | 6                             | 6                             | 6.0     | 0.0
```

**How to read it:**

- **`unique` is the most informative column.** A count of `0` means the column is empty
  for every row — above, `descriptionType` is never populated in search results. A count
  of `1` means the column is the same in every row: here one database, one modification
  timestamp on the catalog side, and no endorsements, warnings or deprecations anywhere
  in the result.
- **The four query-relative columns are usually constant.** `matchedFields`,
  `matchedCustomProperties`, `matchedFieldsTagged` and `ranking` describe how this query
  matched. Above, every view matched exactly one custom property and no columns — which
  is what a `properties` search should produce.
- **Long text has no min/max.** `description` shows `None` because values over 80
  characters are skipped; `unique` still tells you how many distinct descriptions exist.
- **Numeric columns get `avg` and `std_dev`.** For `id` they are meaningless — it is a
  surrogate key — but for `countWarnings` or `ranking` they summarise the result set.

### View Table Schema

```python
print(dsi.schema())     # takes no table name; returns this backend's schema
```

**Output:**
```sql
CREATE TABLE denodo_search_results (
    name TEXT,
    database_name TEXT,
    id INTEGER,
    database_id INTEGER,
    description TEXT,
    descriptionType TEXT,
    categories TEXT,
    tags TEXT,
    lastModificationVdpData TEXT,
    lastModificationIsstData TEXT,
    matchedFields INTEGER,
    matchedCustomProperties INTEGER,
    matchedFieldsTagged INTEGER,
    countEndorsements INTEGER,
    countWarnings INTEGER,
    countDeprecations INTEGER,
    ranking INTEGER
);
```

Types are inferred from the first non-null value in each column, so a column that is
empty for the whole result set is reported as `TEXT`.

### Retrieve a Table

```python
views_df = dsi.get_table("denodo_search_results", collection=True)
```

### Search Loaded Metadata

```python
# Prints ALL cells from rows containing "waste"
dsi.search("waste")
```

**Note:** `search()` displays complete matching rows (all columns), not just matched cells. Searches across:
- Table names
- Column names
- Cell values

### Filter Data

```python
# Views whose name contains "area"
results = dsi.find("name ~~ 'area'")

# Views from one database, as a DataFrame
results = dsi.find("database_name == 'my_database'", collection=True)
```

Supports operators: `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~` (contains), and `(low, high)` ranges

> **Note:** `find()` and `search()` work on the table already in memory. Neither sends another request to Denodo.

### Display Table Preview

```python
dsi.display("denodo_search_results", num_rows=5)
```

**Note:** `display()` shows ALL 17 columns by default, which is wide. Use `display_cols` to limit them:

```python
dsi.display("denodo_search_results", num_rows=10,
            display_cols=["name", "database_name", "description", "categories", "tags"])
```

**Output** (illustrative):
```text
Table: denodo_search_results

name             | database_name | description                                  | categories     | tags
-----------------+---------------+----------------------------------------------+----------------+--------------------
area_type_ref    | sample_db     | Area type reference table (e...              | Reference Data | Reference
admin_form_log   | sample_db     | General administrative forms edit log. Ad... | Reference Data | Operations
ancillary_ref    | sample_db     | Ancillary type reference table. This anc...  | Reference Data | Type, Reference
announcement     | sample_db     | Project and application announcements, in... | Reference Data | Notification
annual_summary   | sample_db     | This table tracks deliverables and any ne... | Planning       | Annual
    ... showing 5 of 350 rows
```

Two things this shows:

- **Long values are truncated with `...`** so rows stay on one line. The full text is in
  the DataFrame from `get_table(..., collection=True)`, not in `display()`.
- **The last line reports the full size**, not the number of rows shown, so you always
  know how much you are not looking at.

### Process to Writable Backend

Convert read-only Denodo data to a local database:

```python
# Query from Denodo
dsi = DSI(
    backend_name="Denodo",
    params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"}
)

# Process the result into a local SQLite database
dsi.process(
    backend_name="Sqlite",
    filename="denodo_cui.db"
)
dsi.close()

# Load the newly created database
local_dsi = DSI(
    backend_name="Sqlite",
    filename="denodo_cui.db"
)

# Query the local database with SQL (not supported in the Denodo backend)
rows = local_dsi.query(
    "SELECT name, database_name FROM denodo_search_results "
    "WHERE tags LIKE '%Reference%' ORDER BY name",
    collection=True
)
local_dsi.close()
```

**Output** (illustrative):
```text
Saved denodo_cui.db
Closing this instance of DSI()
Created an instance of DSI with the Sqlite backend: denodo_cui.db

Printing the result of the query: SELECT name, database_name, categories FROM
denodo_search_results WHERE tags LIKE '%Reference%' ORDER BY name LIMIT 10

name              | database_name | categories
------------------+---------------+----------------
analysis_ref      | sample_db     | Reference Data
area_type_ref     | sample_db     | Reference Data
ancillary_ref     | sample_db     | Reference Data
asset_register    | sample_db     | Reference Data
cost_code_ref     | sample_db     | Reference Data
```

The snapshot is an ordinary SQLite file: the table keeps its name and all 17 columns, so
any SQL tool can read it. Because `tags` and `categories` are stored as comma-joined
names, membership tests use `LIKE '%value%'` rather than `=`.

> **Note:** the snapshot file is written to the directory you run from. Keep it out of
> version control.

### Export Data

Write the table to an external format:

```python
dsi.write(
    filename="views.csv",
    writer_name="Csv",
    table_name="denodo_search_results"
)
```

---

## Example Scripts

The following example scripts demonstrate common workflows with the Denodo backend. All scripts are in `examples/backends/denodo/`.

### 1. load_basic.py

Initialize the Denodo backend with a property search and inspect what was loaded.

- Basic query with the `keywords`, `search_in` and `match` parameters
- Use `list()` to see the loaded table and its size
- Use `summary()` to view per-column statistics
- Use `schema()` to see the table as a `CREATE TABLE` statement
- Introduction to the `denodo_search_results` table

**Note:** this script authenticates from scratch. The first run opens a browser window to log in,
then asks you to paste the redirect URL back at the prompt.

**Example output** (illustrative):

```text
Opening browser for login...
After logging in, paste the full redirect URL here: ...
Created an instance of DSI with the Denodo read-only backend

Table List:

Table: denodo_search_results
  - num of columns: 17
  - num of rows: 350

Table Summary:
... one row per column, see "View Backend Summary" below ...

Schema:
CREATE TABLE denodo_search_results (
    name TEXT,
    database_name TEXT,
    ...
);
Closing this instance of DSI()
```

---

### 2. search_parameters.py

Explore the two parameters that shape every query: where to search, and how to match.

- All five `search_in` scopes on the same word, with the count each returns
- The three `match` modes on a two-word phrase, showing substring vs AND vs OR
- An empty `keywords` value to return the whole catalog
- Authenticate once and reuse the token across queries, instead of logging in each time
- Shows why `match="substring"` is needed for exact-phrase searches

**Scopes:** `name`, `description`, `properties`, `column_names`, `column_descriptions`
**Match modes:** `substring` (whole phrase), `all_words` (AND), `any_words` (OR)

**Example output** (illustrative — the counts depend entirely on your own catalog):

```text
search_in=['name']                 'area' -> 18 views
search_in=['description']          'area' -> 31 views
search_in=['properties']           'area' -> 0 views
search_in=['column_names']         'area' -> 164 views
search_in=['column_descriptions']  'area' -> 25 views

match='substring'   'area type' -> 0 views
match='all_words'   'area type' -> 4 views
match='any_words'   'area type' -> 176 views

keywords=''  (whole catalog) -> 3500 views
```

Three things to read out of that:

- **The scopes are strict.** One word gives five different answers, and a term absent
  from view names may be common among column names. A search that returns nothing is
  often searching the wrong scope, not a term that does not exist.
- **`substring` is literal.** `'area type'` typed with a space matches no view name,
  because view names use underscores, while `all_words` finds the views containing both
  words. The zero is the demonstration, not a failure.
- **An empty keyword returns everything**, which is the cheapest way to see how large
  the catalog is before narrowing a search.

---

### 3. display_columns.py

Preview table data with different column configurations, and hand it to pandas.

- Default behavior shows ALL 17 columns
- Use the `display_cols` parameter to select specific columns
- Create a narrow, readable view of the columns people ask about most
- Retrieve the table as a DataFrame with `get_table(..., collection=True)`
- Use pandas operations on the result, such as `value_counts()`

**Example output** (illustrative, final part):

```text
Shape: (350, 17)

Views per database:
database_name
sample_db    350
Name: count, dtype: int64
```

A single database in `value_counts()` is normal: a narrow search often lands entirely in
one virtual database. Widen the search, or search `column_names`, to see more.

---

### 4. find_and_search.py

Filter and search inside the loaded table, without making another API call.

- Use `find()` for a condition on one named column
- Partial string match with `~~` (contains) and exact match with `==`
- Return results as a DataFrame with `collection=True`
- Use `search()` for free text across table names, column names and every cell
- **Important:** `search()` returns complete matching rows (ALL columns), not just matched cells

**Supported operators:** `>`, `<`, `>=`, `<=`, `==`, `!=`, `~~` (contains), and `(low, high)` ranges

---

### 5. save_snapshot.py

Convert a live search result into a local SQLite database for offline analysis.

- Load a search result from the Data Catalog
- Convert the read-only backend to a writable Sqlite file with `process()`
- Reopen the snapshot as a Sqlite backend
- Query the snapshot with SQL, which the Denodo backend itself does not support
- Offline workflow: no Denodo connection and no token needed once the snapshot exists

**Note:** `tags` and `categories` are stored as comma-joined names, so SQL uses `WHERE tags LIKE '%Waste%'` rather than an equality test.

---

## Notes

- The backend is **metadata-first** and **read-only**
- One search is one `POST /search/metadata`; no other endpoint is called
- Large result sets are paginated internally, and the row count is checked against the server's own total
- One table today: `denodo_search_results`
- A view is identified by `(database_name, name)`; the numeric `id` differs between environments and must not be reused across them
- Multi-query support deduplicates results by `(database_name, name)`
- Empty result sets return an empty table with all columns present (no errors)
- Results are permission-scoped: the token identifies you, so two users may legitimately see different views

---

## Troubleshooting

### Nothing is configured

```python
from dsi.backends.denodo import Denodo
probe = Denodo(only_validate=True)
print(probe.validate_connection())   # False
print(probe.validate_error_msg)      # says what is missing
```

Set `DENODO_BASE_URL` or run `save_config(base_url=...)`, and check the OAuth settings (see **Configuration**).

### Authentication rejected (HTTP 401)

The token has expired or lacks the required scope. Build the backend again to re-authenticate, or pass a fresh `token=`.

### A browser window opens when you did not expect it

Any `DSI(backend_name="Denodo", ...)` without `token=` starts the OAuth flow. In a loop, authenticate once and reuse the token:

```python
first = DSI(backend_name="Denodo", params={"keywords": "area"})
token = first.main_backend_obj.token
# ... pass token=token to every later DSI(...)
```

### Empty results

```python
dsi.list()
# Output: denodo_search_results: (0 rows, 17 cols)
```

Try:
- Broadening the search term, or using `keywords=""` to see the whole catalog
- Searching a different scope — a term that is absent from names may be present in properties
- Switching `match` to `any_words`
- Checking that category and tag ids exist

### `query()` raises NotImplementedError

The Denodo backend holds in-memory tables, not SQL. Use `find()` and `search()`, or save a snapshot with `process()` and query that.

---

## Performance Tips

- Narrow the scope: searching one `search_in` value is faster than several
- Use `limit` when exploring, and drop it when you need the complete result
- Authenticate once and reuse the token across queries
- Cache results locally using `process()` for repeated analysis
- Use `display_cols` in `display()` to keep output readable
- Filter DataFrames after retrieval for conditions `find()` does not express
