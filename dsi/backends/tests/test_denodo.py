"""
Denodo Backend Function Tests

The Denodo Data Catalog is an internal service behind OAuth, so CI can never
reach it. Every test here runs against a mocked catalog: no network, no token,
no browser. This follows the pattern in test_rcsbpdb.py.

Run from the repository root with:

    python -m pytest dsi/backends/tests/test_denodo.py -v

There are deliberately no tests against a real Data Catalog: they would need an
interactive OAuth login, which cannot run unattended. The example scripts in
examples/backends/denodo/ cover that path -- run them after changing anything
that touches the request body or the response shape.
"""

from collections import OrderedDict

import pandas as pd
import pytest

from dsi.backends.denodo import Denodo

from dsi.backends.denodo import (
    Denodo,
    canonical_property_name,
    extract_urls,
    normalize_description,
    normalize_property_value,
    null_if_empty,
)


# Captured before any patching, so a test can restore the real behaviour
REAL_VALIDATE = Denodo.validate_connection

# The registered tables, in registration order. One place to update when a
# new table is added -- the tests below check consistency, not this literal.
EXPECTED_TABLES = ["denodo_search_results", "denodo_databases", "denodo_views"]


# ---------------------------------------------------------------------------
# A fake catalog: one fully-populated view plus generated ones
# ---------------------------------------------------------------------------
GOLDEN_HIT = {
    "id": 101,
    "name": "sample_area_type",
    "description": "Sample area type reference table",
    "descriptionType": None,
    "lastModificationVdpData": "2024-01-04T16:43:49.000+00:00",
    "lastModificationIsstData": "2025-09-09T14:55:55.000+00:00",
    "matchedFields": 0,
    "database": {"databaseId": 5, "databaseName": "sample_db"},
    "categories": [{"id": 80, "name": "Reference Data"}],
    "tags": [{"id": 599, "name": "Waste"}],
    "matchedCustomProperties": 1,
    "matchedFieldsTagged": 0,
    "countEndorsements": 0,
    "countWarnings": 0,
    "countDeprecations": 0,
    "ranking": 6,
}

FAKE_CATALOG = [GOLDEN_HIT] + [
    dict(
        GOLDEN_HIT,
        id=i,
        name=f"view_{i}",
        ranking=i % 7,
        database={"databaseId": i % 3, "databaseName": ["db_a", "db_b", "sample_db"][i % 3]},
        categories=[],
        tags=[],
    )
    for i in range(1, 250)
]

# Databases as the API returns them: one plain description, one null,
# one whitespace-only -- the last two must both normalize to None.
FAKE_DATABASES = [
    {"databaseId": 10, "serverId": 1, "databaseName": "db_a",
     "description": "first database", "descriptionType": None},
    {"databaseId": 5, "serverId": 1, "databaseName": "db_b",
     "description": None, "descriptionType": None},
    {"databaseId": 16, "serverId": 1, "databaseName": "db_empty",
     "description": "   ", "descriptionType": None},
]

# db_a: 7 live + 1 deleted.  db_b: 2.  db_empty: none at all.
FAKE_VIEWS = (
    [{"name": f"v{i}", "db": "db_a", "deleted": False} for i in range(7)]
    + [{"name": f"w{i}", "db": "db_b", "deleted": False} for i in range(2)]
    + [{"name": "gone", "db": "db_a", "deleted": True}]
)


# Every request a test causes is recorded here
REQUESTS = []


def mock_request(self, endpoint, params=None, method="GET", json_body=None):
    """Serve a canned response per endpoint and record the call."""
    # dict(...): the backend reuses one body dict and mutates "offset",
    # so storing a reference would make every recorded call look identical.
    REQUESTS.append({
        "method": method,
        "endpoint": endpoint,
        "body": dict(json_body) if json_body else None,
        "params": dict(params) if params else None,
    })

    if endpoint == "database-management/user/databases":
        return [dict(db) for db in FAKE_DATABASES]

    if endpoint == "views":
        return [dict(v) for v in FAKE_VIEWS]

    offset, limit = json_body["offset"], json_body["limit"]
    return {
        "elementsCount": len(FAKE_CATALOG),
        "elements": FAKE_CATALOG[offset:offset + limit],
    }



@pytest.fixture(scope="session", autouse=True)
def mocked_denodo():
    """Patch the network layer once so tests are deterministic and offline."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(Denodo, "_request", mock_request)
    monkeypatch.setattr(Denodo, "validate_connection", lambda self: True)

    yield

    monkeypatch.undo()


@pytest.fixture(autouse=True)
def clear_requests():
    """Each test starts with an empty call log."""
    REQUESTS.clear()


def make_backend(**params):
    """A loaded backend: no network, no OAuth."""
    return Denodo(url="https://example.org", token="test-token", params=params)


@pytest.fixture(scope="module")
def backend():
    """Shared loaded backend for non-destructive tests."""
    instance = make_backend(keywords="test", search_in=["properties"], match="substring")

    yield instance

    instance.close()

# =============================================================================
# 1) Class contract and flattening
# =============================================================================

def test_class_implements_every_abstract_method():
    """Python refuses to instantiate the class while any is missing."""
    assert Denodo.__abstractmethods__ == frozenset()


def test_schema_matches_flattened_row():
    """SEARCH_SCHEMA and _flatten_hit must be changed together."""
    row = Denodo._flatten_hit(GOLDEN_HIT)

    assert list(row.keys()) == Denodo.SEARCH_SCHEMA


def test_flatten_hit_unnests_and_joins():
    row = Denodo._flatten_hit(GOLDEN_HIT)

    assert row["name"] == "sample_area_type"
    assert row["database_name"] == "sample_db"      # from database.databaseName
    assert row["database_id"] == 5                  # from database.databaseId
    assert row["categories"] == "Reference Data"    # list of dicts -> joined names
    assert row["tags"] == "Waste"


def test_flatten_hit_empty_lists_become_none():
    row = Denodo._flatten_hit(dict(GOLDEN_HIT, categories=[], tags=[]))

    assert row["categories"] is None
    assert row["tags"] is None


def test_initialization(backend):
    assert backend._loaded is True
    assert "denodo_search_results" in backend._cache
    assert list(backend.schemas) == EXPECTED_TABLES



# =============================================================================
# 2) The search request
# =============================================================================

def test_only_post_calls_are_made():
    """Divya's requirement: one POST, no view-details GETs."""
    make_backend(keywords="test")

    assert REQUESTS
    assert all(r["method"] == "POST" for r in REQUESTS)
    assert all(r["endpoint"] == "search/metadata" for r in REQUESTS)


def test_pagination_collects_every_hit():
    d = make_backend(keywords="test")

    assert len(d.get_table("denodo_search_results")) == len(FAKE_CATALOG)
    assert [r["body"]["offset"] for r in REQUESTS] == [0, 100, 200]


def test_limit_stops_pagination_early():
    d = make_backend(keywords="test", limit=5)

    assert len(d.get_table("denodo_search_results")) == 5
    assert len(REQUESTS) == 1


def test_body_fixes_the_fields_the_api_requires():
    make_backend(keywords="x", search_in=["properties"], match="substring")
    body = REQUESTS[0]["body"]

    assert body["whereToSearchList"] == ["PROPERTY_VALUE"]
    assert body["searchType"] == "EXACT_MATCH"
    assert body["elementType"] == "VIEWS"                  # the only value this API serves
    assert body["withEndorsements"] is False               # these filter, they do not include
    assert body["withWarnings"] is False
    assert body["withDeprecations"] is False
    for key in ("categoryIds", "tagIds", "databaseIds"):   # omitting any -> HTTP 400
        assert key in body


def test_defaults_come_from_the_class_attributes():
    make_backend(keywords="x")
    body = REQUESTS[0]["body"]

    assert body["whereToSearchList"] == [
        Denodo.SEARCH_SCOPES[s] for s in Denodo.DEFAULT_SEARCH_IN
    ]
    assert body["searchType"] == Denodo.MATCH_TYPES[Denodo.DEFAULT_MATCH]


def test_categories_and_tags_are_passed_through():
    make_backend(keywords="x", categories=[80], tags=[599])
    body = REQUESTS[0]["body"]

    assert body["categoryIds"] == [80]
    assert body["tagIds"] == [599]


@pytest.mark.parametrize("params", [
    {"keywords": "x", "search_in": ["nope"]},
    {"keywords": "x", "match": "exactly"},
])
def test_invalid_parameter_values_are_rejected(params):
    """__init__ wraps load failures, so the ValueError arrives as RuntimeError."""
    with pytest.raises(RuntimeError) as excinfo:
        make_backend(**params)

    assert "Valid:" in str(excinfo.value)


def test_duplicate_views_are_removed_across_queries():
    d = Denodo(
        url="https://example.org",
        token="test-token",
        params=[{"keywords": "a"}, {"keywords": "b"}],
    )
    df = d.get_table("denodo_search_results")
    pairs = list(zip(df["database_name"], df["name"]))

    assert len(df) == len(FAKE_CATALOG)     # not twice that
    assert len(pairs) == len(set(pairs))



# =============================================================================
# 3) Table access, summary, display
# =============================================================================

def test_get_table(backend):
    df = backend.get_table("denodo_search_results")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == Denodo.SEARCH_SCHEMA

    as_dict = backend.get_table("denodo_search_results", dict_return=True)
    assert isinstance(as_dict, OrderedDict)

    with pytest.raises(ValueError):
        backend.get_table("nonexistent_table")


def test_empty_result_still_has_every_column():
    d = make_backend(keywords="x")
    d._cache["denodo_search_results"] = d._rows_to_table([], Denodo.SEARCH_SCHEMA)

    assert d.get_table("denodo_search_results").shape == (0, len(Denodo.SEARCH_SCHEMA))


def test_list_and_num_tables(backend):
    assert backend.list(collection=True) == list(backend.schemas)
    assert backend.num_tables() == len(backend.schemas)


def test_get_schema(backend):
    schema = backend.get_schema()

    assert "CREATE TABLE denodo_search_results" in schema
    assert "name TEXT" in schema
    assert "id INTEGER" in schema


def test_summary_is_one_row_per_column(backend):
    summary = backend.summary("denodo_search_results")

    assert list(summary.columns) == [
        "column", "type", "unique", "min", "max", "avg", "std_dev",
    ]
    assert len(summary) == len(Denodo.SEARCH_SCHEMA)

    all_tables = backend.summary()
    assert all_tables[0] == EXPECTED_TABLES


def test_display_sets_max_rows(backend):
    """core.py reads attrs['max_rows'] to print 'showing N of M rows'."""
    out = backend.display("denodo_search_results", num_rows=10)

    assert len(out) == 10
    assert out.attrs["max_rows"] == len(FAKE_CATALOG)


def test_display_columns(backend):
    out = backend.display("denodo_search_results", num_rows=5,
                          display_cols=["name", "tags"])

    assert list(out.columns) == ["name", "tags"]

    with pytest.raises(ValueError):
        backend.display("denodo_search_results", display_cols=["nope"])


# =============================================================================
# 4) Find methods
# =============================================================================

def test_find_table_column_cell(backend):
    assert any(v.t_name == "denodo_search_results" for v in backend.find_table("denodo"))
    assert any(v.c_name == ["database_name"] for v in backend.find_column("database"))
    assert backend.find_cell("sample_area_type")


def test_find_cell_row_mode_shape(backend):
    """dsi.search() builds pd.DataFrame([val.value], columns=val.c_name)."""
    hits = backend.find_cell("sample_area_type", row=True)

    assert hits
    val = hits[0]
    assert val.type == "row"
    assert isinstance(val.value, list)
    assert len(val.value) == len(val.c_name) == len(Denodo.SEARCH_SCHEMA)


def test_find_accepts_a_non_string_query(backend):
    """dsi.search(101) calls all three find methods with the same value."""
    assert backend.find(101)


def test_find_column_range(backend):
    values = [v.value for v in backend.find_column("ranking", range=True)]

    assert values
    assert len(values[0]) == 2          # [min, max]


@pytest.mark.parametrize(("column", "relation"), [
    ("ranking", "> 5"),
    ("ranking", ">= 3"),
    ("ranking", "(2, 4)"),
    ("name", "~ 'area'"),
    ("database_name", "= 'sample_db'"),
])
def test_find_relation(backend, column, relation):
    result = backend.find_relation(column, relation)

    assert result, f"no rows matched {column} {relation}"
    assert isinstance(result[0].value, list)


# =============================================================================
# 5) Read-only enforcement, lifecycle, probing
# =============================================================================

def test_read_only():
    d = make_backend(keywords="x")

    with pytest.raises(NotImplementedError):
        d.ingest_artifacts({})

    with pytest.raises(NotImplementedError):
        d.query_artifacts("SELECT 1")


def test_close_resets_state():
    d = make_backend(keywords="x")
    assert len(d.get_table("denodo_search_results")) > 0

    d.close()

    assert d._loaded is False
    assert d.get_table("denodo_search_results").shape == (0, len(Denodo.SEARCH_SCHEMA))


def test_probe_without_configuration(monkeypatch):
    """dsi.list_backends() builds a probe: it must not raise or start OAuth."""
    import dsi.backends.denodo as denodo_module

    monkeypatch.setattr(Denodo, "validate_connection", REAL_VALIDATE)
    monkeypatch.delenv("DENODO_BASE_URL", raising=False)
    monkeypatch.setattr(denodo_module, "_load_config", lambda: {})

    probe = Denodo(only_validate=True)

    assert probe.base_url is None
    assert probe.validate_connection() is False


# =============================================================================
# 6) Normalization (contract section 3)
# =============================================================================

@pytest.mark.parametrize(("raw", "property_type", "expected"), [
    ("user&#64;example.org", "RICH_TEXT", "user@example.org"),
    ('<p><strong> <a href="https://example.org/x">Baker, Alex</a></strong></p>',
     "RICH_TEXT", "Baker, Alex"),
    ("Infrequently / Ad hoc basis", "ENUMERATION", "Infrequently / Ad hoc basis"),
    ("WASTE_READ", "RICH_TEXT", "WASTE_READ"),
])
def test_normalize_property_value(raw, property_type, expected):
    """The four cases the data contract requires (section 3.2)."""
    assert normalize_property_value(raw, property_type) == expected


def test_unescaping_happens_after_stripping():
    """Unescaping first would build a tag that stripping then deletes."""
    assert normalize_property_value("a &lt;b&gt; c", "RICH_TEXT") == "a <b> c"


def test_tags_are_stripped_only_for_rich_text():
    assert normalize_property_value("<b>x</b>", "RICH_TEXT") == "x"
    assert normalize_property_value("<b>x</b>", "LONG_TEXT") == "<b>x</b>"


def test_normalize_property_value_handles_empty_and_none():
    assert normalize_property_value(None, "RICH_TEXT") is None
    assert normalize_property_value("   ", "RICH_TEXT") is None


@pytest.mark.parametrize(("raw", "expected"), [
    # Anchor form -- what property values use.
    ('<a href="https://example.org/a">a</a>', ["https://example.org/a"]),
    # Two anchors, order preserved.
    ('<a href="https://example.org/a">a</a> and <a href=\'https://example.org/b\'>b</a>',
     ["https://example.org/a", "https://example.org/b"]),
    # Bare form -- what descriptions use. Returned [] before the fix, silently.
    ("Source: https://example.org/doc.html?c_n=x.",
     ["https://example.org/doc.html?c_n=x"]),
    # Both forms in one value: href first, and the bare match must not duplicate it.
    ('<a href="https://example.org/a">a</a> see also https://example.org/b',
     ["https://example.org/a", "https://example.org/b"]),
    ("no links here", []),
    (None, []),
])
def test_extract_urls_handles_both_url_forms(raw, expected):
    assert extract_urls(raw) == expected



@pytest.mark.parametrize(("value", "expected"), [
    ("", None), ("   ", None), ("NOLINK", None), ("ok", "ok"), (None, None),
])
def test_null_policy(value, expected):
    assert null_if_empty(value) == expected


def test_canonical_property_name_keeps_trailing_punctuation():
    assert canonical_property_name("Details", "Business Unit:") == "Details/Business Unit:"


def test_normalize_description_splits_text_and_url():
    text, url = normalize_description(
        '<p>Reference table. <a href="https://example.org/doc">docs</a></p>'
    )
    assert text == "Reference table. docs"
    assert url == "https://example.org/doc"

    assert normalize_description(None) == (None, None)
    assert normalize_description("Plain text") == ("Plain text", None)


# =============================================================================
# 7) The databases table
# =============================================================================
def test_databases_table_is_registered_and_empty_by_default():
    """Registering a schema is enough: the table exists before any fetch."""
    backend = make_backend(keywords="test")
    table = backend.get_table("denodo_databases")
    assert list(table.columns) == Denodo.DATABASES_SCHEMA
    assert table.shape == (0, len(Denodo.DATABASES_SCHEMA))
    backend.close()


def test_databases_table_has_one_row_per_database():
    backend = make_backend(databases=[])
    table = backend.get_table("denodo_databases")
    assert table["db_name"].tolist() == ["db_a", "db_b", "db_empty"]
    assert table["database_id"].tolist() == [10, 5, 16]
    assert table["server_id"].tolist() == [1, 1, 1]
    backend.close()


def test_missing_description_is_none_however_it_is_spelled():
    """None and a whitespace-only string are both 'no description'."""
    backend = make_backend(databases=[])
    assert backend.get_table("denodo_databases")["description"].tolist() == [
        "first database", None, None,
    ]
    backend.close()


def test_view_count_counts_live_views_only():
    """A deleted view is not counted; a database with no views scores 0."""
    backend = make_backend(databases=[])
    assert backend.get_table("denodo_databases")["view_count"].tolist() == [7, 2, 0]
    backend.close()


def test_databases_can_be_restricted_to_a_subset():
    backend = make_backend(databases=["db_b"])
    table = backend.get_table("denodo_databases")
    assert table["db_name"].tolist() == ["db_b"]
    assert table["view_count"].tolist() == [2]
    backend.close()



def test_unknown_database_is_rejected_before_any_fetch():
    """An unknown name must not reach the API, which answers 500, not 404."""
    with pytest.raises(RuntimeError) as err:
        make_backend(databases=["db_a", "nope"])
    assert "nope" in str(err.value)


def test_provenance_columns_are_filled():
    backend = make_backend(databases=[])
    table = backend.get_table("denodo_databases")
    assert all(value == "example.org" for value in table["source_env"])
    assert len(set(table["fetched_at"])) == 1      # one timestamp per fetch
    assert all(t and t.endswith("+00:00") for t in table["fetched_at"])
    backend.close()


def test_the_databases_path_makes_exactly_two_calls():
    """One call for the databases, one for the counts -- no per-view fetches."""
    make_backend(databases=[])
    assert [r["endpoint"] for r in REQUESTS] == [
        "database-management/user/databases", "views",
    ]



# =============================================================================
# 8) The views table
# =============================================================================
def test_views_table_is_registered_and_empty_by_default():
    """Registering a schema is enough: the table exists before any fetch."""
    backend = make_backend(keywords="test")
    table = backend.get_table("denodo_views")
    assert list(table.columns) == Denodo.VIEWS_SCHEMA
    assert table.shape == (0, len(Denodo.VIEWS_SCHEMA))
    backend.close()


def test_build_view_rows_maps_every_column():
    """One flattened search row maps to one VIEWS_SCHEMA row, keys in order."""
    backend = Denodo(url="https://example.org", token="x", only_validate=True)
    flat = [{
        "name": "demo_view",
        "database_name": "demo_db",
        "id": 101,
        "description": "A demo table.",
        "categories": "Reference Data",
        "tags": None,
        "lastModificationVdpData": "2024-01-04T16:43:49.000+00:00",
    }]

    row = backend._build_view_rows(flat)[0]

    assert list(row) == Denodo.VIEWS_SCHEMA
    assert row["view_name"] == "demo_view"
    assert row["db_name"] == "demo_db"
    assert row["element_id"] == 101
    assert row["tags"] is None


def test_build_view_rows_extracts_a_bare_url_from_the_description():
    """The URL form descriptions actually use -- this returned None before rev 22."""
    backend = Denodo(url="https://example.org", token="x", only_validate=True)
    flat = [{
        "name": "v",
        "database_name": "db",
        "id": 1,
        "description": "A table. Source: https://example.org/doc.html?c_n=x.",
    }]

    row = backend._build_view_rows(flat)[0]

    assert row["documentation_url"] == "https://example.org/doc.html?c_n=x"
    # The URL stays in the prose: removing it would leave "Source: " dangling.
    assert "https://example.org/doc.html" in row["description"]


def test_build_view_rows_shares_one_timestamp():
    """Provenance describes the fetch, not the row."""
    backend = Denodo(url="https://example.org", token="x", only_validate=True)
    flat = [{"name": f"v{i}", "database_name": "db", "id": i} for i in range(3)]

    rows = backend._build_view_rows(flat)

    assert len({row["fetched_at"] for row in rows}) == 1
    assert all(row["source_env"] == "example.org" for row in rows)


def test_views_path_fills_the_table_and_deduplicates():
    """params={'views': ...} fills denodo_views, one row per (db_name, view_name)."""
    backend = make_backend(views="test")
    table = backend.get_table("denodo_views")

    assert len(table) > 0
    assert list(table.columns) == Denodo.VIEWS_SCHEMA
    pairs = list(zip(table["db_name"], table["view_name"]))
    assert len(pairs) == len(set(pairs))
    backend.close()







