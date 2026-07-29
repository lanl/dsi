"""
NDP Backend Function Tests

Essential tests for NDP backend core functionality.

Remote-server calls are minimized by sharing NDP instances through
module-scoped pytest fixtures. Tests that close or otherwise invalidate
an instance use dedicated fixtures.
"""

from collections import OrderedDict

import pandas as pd
import pytest

from dsi.backends.ndp import NDP

pytestmark = pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def backend():
    """
    Shared backend for non-destructive tests.

    Using one broad query avoids repeatedly sending equivalent queries to the
    remote server. Do not call close() on this fixture from an individual test.
    """
    instance = NDP(
        params={
            "keywords": "climate temperature data",
            "limit": 10,
        }
    )

    yield instance

    instance.close()


@pytest.fixture(scope="module")
def filtered_backend():
    """
    Shared backend for filter and multiple-query tests.

    NDP may perform one remote request for each entry, but the resulting
    backend can be reused by every test that exercises filtered loading and
    deduplication.
    """
    instance = NDP(
        params=[
            {
                "keywords": "climate",
                "organization": "California Landscape Metrics",
                "limit": 5,
            },
            {
                "keywords": "temperature",
                "tags": ["climate", "weather"],
                "limit": 5,
            },
            {
                "keywords": "data",
                "groups": ["data_hub_cc_wstc"],
                "limit": 5,
            },
            {
                "keywords": "data",
                "formats": ["CSV", "JSON"],
                "limit": 5,
            },
            {
                "organization": "USGS",
                "limit": 5,
            },
            {
                "tags": ["temperature"],
                "limit": 5,
            },
        ]
    )

    yield instance

    instance.close()


@pytest.fixture(scope="module")
def empty_backend():
    """Shared backend for tests requiring an empty query result."""
    instance = NDP(
        params={
            "keywords": "zzzzznonexistentkeywordzzzzz",
            "limit": 10,
        }
    )

    yield instance

    instance.close()


# =============================================================================
# 1) Initialization & Connection
# =============================================================================

def test_ndp_initialization(backend):
    """Test NDP backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "datasets" in backend._cache


def test_ndp_validate_connection(backend):
    """Test connection validation to CKAN API."""
    assert backend.validate_connection() is True


# =============================================================================
# 2) Data Loading & Structure
# =============================================================================

def test_ndp_unified_resources_table(backend):
    """Test that resources from all datasets are in one unified table."""
    table_names = list(backend._cache.keys())

    assert "datasets" in table_names

    if "resources" not in table_names:
        pytest.skip("The query returned no resource records")

    resources = backend._cache["resources"]

    assert "dataset_id" in resources
    assert "resource_id" in resources
    assert "url" in resources

    datasets = backend._cache["datasets"]
    dataset_ids = set(datasets["id"])
    resource_dataset_ids = set(resources["dataset_id"])

    assert resource_dataset_ids.issubset(dataset_ids)


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_ndp_query_artifacts_not_supported(backend):
    """Test that query_artifacts raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("SELECT * FROM datasets")


def test_ndp_get_table(backend):
    """Test getting table data as DataFrame or OrderedDict."""
    df = backend.get_table("datasets", dict_return=False)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    dict_data = backend.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)

    with pytest.raises(ValueError):
        backend.get_table("nonexistent_table")


def test_ndp_get_schema(backend):
    """Test get_schema returns SQL-style CREATE TABLE format."""
    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE" in schema
    assert "datasets" in schema

    datasets_schema = backend.get_schema("datasets")
    assert "CREATE TABLE datasets" in datasets_schema


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_ndp_find_methods(backend):
    """Test all find methods work correctly."""
    results = backend.find("title")
    assert isinstance(results, list)

    tables = backend.find_table("datasets")
    assert isinstance(tables, list)
    assert any("datasets" in table.t_name for table in tables)

    columns = backend.find_column("title")
    assert isinstance(columns, list)
    assert any("title" in column.c_name for column in columns)

    cells = backend.find_cell("climate")
    assert isinstance(cells, list)


@pytest.mark.parametrize(
    ("column", "relation"),
    [
        ("num_resources", "> 2"),
        ("title", "~~ climate"),
        ("num_resources", ">= 3"),
        ("num_resources", "<= 8"),
        ("num_resources", "== 5"),
        ("num_resources", "!= 0"),
    ],
)
def test_ndp_find_relation(backend, column, relation):
    """Test find_relation with various operators."""
    result = backend.find_relation(column, relation)
    assert isinstance(result, list)


# =============================================================================
# 5) URL Validation
# =============================================================================

def test_ndp_validate_urls(backend):
    """Test URL validation for resources table."""
    if "resources" not in backend._cache:
        pytest.skip("The query returned no resource records")

    backend.validate_urls()

    resources_table = backend._cache["resources"]

    if "url" not in resources_table or len(resources_table["url"]) == 0:
        pytest.skip("The resources table contains no URLs")

    assert "url_valid" in resources_table
    assert all(
        isinstance(value, bool)
        for value in resources_table["url_valid"]
    )


# =============================================================================
# 6) Display & Summary
# =============================================================================

def test_ndp_list(backend):
    """Test list method returns table names."""
    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "datasets" in table_names


def test_ndp_summary(backend):
    """Test summary returns proper format with SQL-style types."""
    summary_list = backend.summary()

    assert isinstance(summary_list, list)
    assert len(summary_list) >= 2

    for dataframe in summary_list[1:]:
        assert isinstance(dataframe, pd.DataFrame)
        assert "column" in dataframe.columns
        assert "type" in dataframe.columns

    summary_single = backend.summary("datasets")
    assert isinstance(summary_single, pd.DataFrame)


def test_ndp_display(backend):
    """Test display method for tables."""
    backend.display("datasets", num_rows=5)
    backend.display(
        "datasets",
        num_rows=5,
        display_cols=["title", "organization"],
    )


# =============================================================================
# 7) Filtering and Multiple Queries
# =============================================================================

def test_ndp_filters(filtered_backend):
    """Test loading data from the configured filtered queries."""
    assert filtered_backend._loaded is True
    assert "datasets" in filtered_backend._cache


def test_ndp_multiple_queries_deduplicate_datasets(filtered_backend):
    """Test that results from multiple queries are deduplicated."""
    datasets = filtered_backend._cache.get("datasets", {})

    if "id" not in datasets:
        pytest.skip("The queries returned no dataset IDs")

    dataset_ids = datasets["id"]

    assert len(dataset_ids) == len(set(dataset_ids))


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_ndp_read_only(backend):
    """Test that write operations raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})

    with pytest.raises(NotImplementedError):
        backend.overwrite_table("datasets", pd.DataFrame())


# =============================================================================
# 9) Lifecycle
# =============================================================================

def test_ndp_close():
    """
    Test that close() properly resets backend state.

    This test must use its own instance because close() destructively mutates
    the backend and would invalidate a shared fixture.
    """
    backend = NDP(
        params={
            "keywords": "climate",
            "limit": 1,
        }
    )

    assert backend._loaded is True
    assert len(backend._cache) > 0

    backend.close()

    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._dataset_id_map) == 0


# =============================================================================
# 10) Edge Cases
# =============================================================================

def test_ndp_empty_results(empty_backend):
    """Test handling of queries that return no results."""
    datasets = empty_backend._cache.get("datasets", {})

    if datasets and "id" in datasets:
        assert len(datasets["id"]) == 0