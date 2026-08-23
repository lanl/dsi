"""
Maglab Backend Function Tests

Tests Maglab backend methods directly without Terminal integration.
Mirrors structure of test_osti.py
"""

import pytest
import pandas as pd
from collections import OrderedDict
from copy import deepcopy
from unittest.mock import patch
from dsi.backends.maglab import Maglab

pytestmark = pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")

# Open one shared Maglab backend using one combined query.
# Maglab initialization performs one validate_connection request, one node
# metadata request, N paginated file-listing requests (page[size]=100), and
# reuses the already-fetched node metadata for relationships (no extra call).
backend = Maglab(params={"node_id": "8r2b3"})

# =============================================================================
# 0) pytest initialization to avoid additional Maglab calls
# =============================================================================

# No rate-limit delay is needed because all tests below reuse the shared backend.
# Any method that would make another network request is mocked in its test.

# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================


def test_maglab_initialization():
    """Test Maglab backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "datasets" in backend._cache
    assert "files" in backend._cache
    assert "relationships" in backend._cache


def test_maglab_validate_connection():
    """Successful initialization confirms validate_connection already passed."""
    assert backend._loaded is True
    assert backend.base_url == "https://api.osf.io/v2"


def test_maglab_invalid_url():
    """Test that invalid URL raises appropriate error."""
    with pytest.raises(ValueError):
        Maglab(url="not-a-valid-url")


# =============================================================================
# 2) Data Loading and Structure
# =============================================================================


def test_maglab_load_initial_data():
    """Test that initial data load creates proper structure."""
    assert isinstance(backend._cache, OrderedDict)
    assert isinstance(backend._cache["datasets"], OrderedDict)
    assert isinstance(backend._cache["files"], OrderedDict)
    assert isinstance(backend._cache["relationships"], OrderedDict)


def test_maglab_datasets_columns():
    """Test datasets table has expected columns."""
    dataset_cols = list(backend._cache["datasets"].keys())

    for col in ["node_id", "title", "category", "public", "license", "raw_attributes"]:
        assert col in dataset_cols


def test_maglab_files_columns():
    """Test files table has expected columns."""
    file_cols = list(backend._cache["files"].keys())

    for col in [
        "node_id",
        "osf_file_id",
        "name",
        "materialized_path",
        "size_bytes",
        "download_url",
        "raw_attributes",
    ]:
        assert col in file_cols


def test_maglab_relationships_columns():
    """Test relationships table has expected columns."""
    rel_cols = list(backend._cache["relationships"].keys())

    for col in ["node_id", "relationship_name", "href", "has_inline_data"]:
        assert col in rel_cols


def test_maglab_relationships_excludes_files():
    """Test 'files' relationship is never present since it's expanded separately."""
    rel_names = backend._cache["relationships"]["relationship_name"]
    assert "files" not in rel_names


def test_maglab_bad_params_type():
    """Test invalid params without making another live validation request."""
    with patch.object(Maglab, "validate_connection", return_value=True):
        with pytest.raises(RuntimeError):
            Maglab(params="not-a-dict-or-list")


# =============================================================================
# 3) Query Operations
# =============================================================================


def test_maglab_query_artifacts():
    """Maglab is a non-SQL backend, so query_artifacts is not supported."""
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("title.notnull()")


def test_maglab_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    for table_name in ["datasets", "files", "relationships"]:
        df = backend.get_table(table_name, dict_return=False)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        dict_data = backend.get_table(table_name, dict_return=True)
        assert isinstance(dict_data, OrderedDict)
        assert len(dict_data) > 0


def test_maglab_get_table_invalid_name():
    """Test that requesting a nonexistent table raises ValueError."""
    with pytest.raises(ValueError):
        backend.get_table("fake_table")


def test_maglab_get_schema():
    """Test that get_schema returns informative schema."""
    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE datasets" in schema
    assert "CREATE TABLE files" in schema
    assert "CREATE TABLE relationships" in schema

    assert "node_id" in schema
    assert "title" in schema
    assert "osf_file_id" in schema
    assert "relationship_name" in schema


# =============================================================================
# 4) Find Operations
# =============================================================================


def test_maglab_find():
    """Test general find operation across all levels."""
    results = backend.find("title")
    assert isinstance(results, list)


def test_maglab_find_table():
    """Test finding tables by name."""
    tables_found = backend.find_table("files")

    assert isinstance(tables_found, list)
    assert len(tables_found) > 0
    assert any("files" in t.t_name for t in tables_found)


def test_maglab_find_column():
    """Test finding columns by name."""
    columns_found = backend.find_column("node_id")

    assert isinstance(columns_found, list)
    assert len(columns_found) > 0
    assert any("node_id" in c.c_name for c in columns_found)


def test_maglab_find_cell():
    """Test finding cells by value."""
    cells_found = backend.find_cell("8r2b3")

    assert isinstance(cells_found, list)
    assert len(cells_found) > 0
    assert all(hasattr(cell, "type") and cell.type == "cell" for cell in cells_found)


def test_maglab_find_relation():
    """Test relation finding against the loaded files table."""
    results = backend.find_relation("size_bytes", ">1000000")

    assert isinstance(results, list)
    for result in results:
        assert result.type == "relation"
        assert result.t_name == "files"


def test_maglab_find_relation_no_matches():
    """Test relation finding with an operator that yields no matches."""
    results = backend.find_relation("size_bytes", ">999999999999")

    assert isinstance(results, list)
    assert len(results) == 0


# =============================================================================
# 5) Multi-node / Deduplication Behavior
# =============================================================================


def test_maglab_multiple_query_params_are_deduplicated():
    """
    Test that a duplicate node_id in a list of params dedupes to one dataset row.

    The underlying HTTP call (_request) is mocked and fed cached JSON already
    captured from the shared `backend`'s live initialization, so this test
    performs zero additional network round-trips.
    """
    node_data = backend._cache["datasets"]["raw_attributes"][0]
    cached_response = {"data": node_data}

    provider_listing = {
        "data": [
            {
                "attributes": {"provider": "osfstorage"},
                "relationships": {
                    "files": {"links": {"related": {"href": "https://api.osf.io/v2/fake/files/"}}}
                },
            }
        ]
    }
    empty_page = {"data": [], "links": {"next": None}}

    def fake_request(self, endpoint_or_url, params=None):
        endpoint = str(endpoint_or_url)
        if "nodes/8r2b3/files/" in endpoint:
            return provider_listing
        if endpoint.startswith("http"):
            return empty_page
        return cached_response

    with patch.object(Maglab, "validate_connection", return_value=True), \
         patch.object(Maglab, "_request", side_effect=fake_request, autospec=True):
        local = Maglab(params=[{"node_id": "8r2b3"}, {"node_id": "8r2b3"}])

    datasets = local.get_table("datasets", dict_return=False)

    assert len(datasets) == 1
    assert datasets["node_id"].nunique() == 1
    assert datasets.iloc[0]["node_id"] == "8r2b3"


# =============================================================================
# 6) List and Summary
# =============================================================================


def test_maglab_list():
    """Test list method returns table names."""
    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "datasets" in table_names
    assert "files" in table_names
    assert "relationships" in table_names


def test_maglab_num_tables():
    """Test num_tables does not raise exception."""
    backend.num_tables()


def test_maglab_num_tables_with_table_name():
    """Test num_tables returns row count for a specific table."""
    count = backend.num_tables(table_name="files")

    assert isinstance(count, int)
    assert count >= 0


def test_maglab_summary():
    """Test summary returns table metadata."""
    summary_list = backend.summary()

    assert isinstance(summary_list, list)
    assert len(summary_list) == 4
    assert isinstance(summary_list[0], list)
    assert "datasets" in summary_list[0]
    assert "files" in summary_list[0]
    assert "relationships" in summary_list[0]

    for df in summary_list[1:]:
        assert isinstance(df, pd.DataFrame)


def test_maglab_summary_single_table():
    """Test summary for individual tables."""
    for table_name in ["datasets", "files", "relationships"]:
        summary_single = backend.summary(table_name)

        assert isinstance(summary_single, pd.DataFrame)
        assert "column" in summary_single.columns


def test_maglab_summary_invalid_table():
    """Test summary rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.summary("fake_table")


# =============================================================================
# 7) Display
# =============================================================================


def test_maglab_display():
    """Test display method for the files table."""
    result = backend.display("files", num_rows=10)

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10


def test_maglab_display_cols():
    """Test display with selected columns."""
    result = backend.display(
        "files",
        num_rows=5,
        display_cols=["osf_file_id", "name"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["osf_file_id", "name"]


def test_maglab_display_invalid_table():
    """Test display rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.display("fake_table")


# =============================================================================
# 8) Process Artifacts
# =============================================================================


def test_maglab_process_artifacts():
    """Test that processed artifacts have correct structure."""
    artifacts = backend.process_artifacts()

    assert isinstance(artifacts, OrderedDict)
    assert "datasets" in artifacts
    assert "files" in artifacts
    assert "relationships" in artifacts
    assert isinstance(artifacts["datasets"], OrderedDict)
    assert isinstance(artifacts["files"], OrderedDict)
    assert isinstance(artifacts["relationships"], OrderedDict)


# =============================================================================
# 9) Read-Only Enforcement
# =============================================================================


def test_maglab_ingest_artifacts():
    """Test that ingest_artifacts raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


# =============================================================================
# 10) Extension Filtering (pure function, no network calls)
# =============================================================================


def test_maglab_normalize_ext_string():
    """Test _normalize_ext with a single string extension."""
    assert backend._normalize_ext("tdms") == [".tdms"]
    assert backend._normalize_ext(".tdms") == [".tdms"]


def test_maglab_normalize_ext_list():
    """Test _normalize_ext with a list of extensions."""
    assert backend._normalize_ext([".csv", "TXT"]) == [".csv", ".txt"]


def test_maglab_normalize_ext_mixed_case_and_dots():
    """Test _normalize_ext with mixed dot-prefixed and bare, mixed-case extensions."""
    assert backend._normalize_ext(["TDMS", ".CSV", "Json"]) == [".tdms", ".csv", ".json"]


def test_maglab_normalize_ext_none():
    """Test _normalize_ext with None or empty input returns None."""
    assert backend._normalize_ext(None) is None
    assert backend._normalize_ext("") is None
    assert backend._normalize_ext([]) is None


# =============================================================================
# 11) Lifecycle
# =============================================================================


def test_maglab_notebook():
    """Test that notebook() throws NotImplementedError."""
    try:
        backend.notebook()
        assert False
    except NotImplementedError: # should throw error
        assert True


def test_maglab_close():
    """Test close() without destroying the shared backend used by other tests."""
    local = Maglab.__new__(Maglab)
    local._loaded = True
    local._cache = OrderedDict(
        {"datasets": OrderedDict({"node_id": ["8r2b3"]})}
    )

    local.close()

    assert local._loaded is False
    assert len(local._cache) == 0
