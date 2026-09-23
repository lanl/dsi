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

# Captured before any patching, so a test can restore the real behaviour
REAL_VALIDATE = Denodo.validate_connection

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

# Every request a test causes is recorded here
REQUESTS = []


def mock_request(self, endpoint, params=None, method="GET", json_body=None):
    """Serve one page from FAKE_CATALOG and record the call."""
    # dict(json_body): the backend reuses one body dict and mutates "offset",
    # so storing a reference would make every recorded call look identical.
    REQUESTS.append({"method": method, "endpoint": endpoint, "body": dict(json_body)})
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
    assert list(backend.schemas) == ["denodo_search_results"]
    assert "denodo_search_results" in backend._cache


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
    assert backend.list(collection=True) == ["denodo_search_results"]
    assert backend.num_tables() == 1


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
    assert all_tables[0] == ["denodo_search_results"]


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




