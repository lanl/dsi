"""
OSTI Backend Function Tests

Tests OSTI backend methods directly without Terminal integration.
Mirrors structure of test_sqlite.py
"""

import pytest
import pandas as pd
from collections import OrderedDict
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
from dsi.backends.osti import OSTI

pytestmark = pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")

# Open one shared OSTI backend using one combined query.
# OSTI initialization performs one validate_connection request and one data request.
backend = OSTI(
    params={
        "q": "wildfire",
        "author": "Linn",
        "title": "wildfire",
        "rows": 6,
        "page": 1,
    }
)

# =============================================================================
# 0) pytest initialization to avoid additional OSTI calls
# =============================================================================

# No rate-limit delay is needed because all tests below reuse the shared backend.
# Any method that would make another network request is mocked in its test.

# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================


def test_osti_initialization():
    """Test OSTI backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "records" in backend._cache


def test_osti_validate_connection():
    """Successful initialization confirms validate_connection already passed."""
    assert backend._loaded is True
    assert backend.base_url == "https://www.osti.gov/api/v1"


def test_osti_invalid_url():
    """Test that invalid URL raises appropriate error."""
    with pytest.raises(ValueError):
        OSTI(url="not-a-valid-url")


# =============================================================================
# 2) Data Loading and Structure
# =============================================================================


def test_osti_load_initial_data():
    """Test that initial data load creates proper structure."""
    assert isinstance(backend._cache, OrderedDict)
    assert "records" in backend._cache
    assert isinstance(backend._cache["records"], OrderedDict)

    record_cols = list(backend._cache["records"].keys())
    assert "osti_id" in record_cols
    assert "title" in record_cols
    assert "authors" in record_cols
    assert "subjects" in record_cols
    assert "has_fulltext" in record_cols


def test_osti_combined_query_result():
    """Test that the combined q, author, and title request returns matching records."""
    df = backend.get_table("records", dict_return=False)

    assert not df.empty
    assert 1 <= len(df) <= 6

    # Basic record structure
    assert df["osti_id"].notna().all()
    assert df["title"].notna().all()
    assert df["authors"].notna().all()

    # Verify OSTI actually applied the field-specific filters.
    assert df["authors"].apply(
        lambda value: "linn" in str(value).lower()
    ).all()

    assert df["title"].apply(
        lambda value: "wildfire" in str(value).lower()
    ).all()


def test_osti_bad_params_type():
    """Test invalid params without making another live validation request."""
    with patch.object(OSTI, "validate_connection", return_value=True):
        with pytest.raises(RuntimeError):
            OSTI(params="not-a-dict-or-list")


# =============================================================================
# 3) Query Operations
# =============================================================================


def test_osti_query_artifacts():
    """OSTI is a non-SQL backend, so query_artifacts is not supported."""
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("title.notnull()")


def test_osti_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    df = backend.get_table("records", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    dict_data = backend.get_table("records", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0


def test_osti_get_table_invalid_name():
    """Test that requesting a non-records table raises ValueError."""
    with pytest.raises(ValueError):
        backend.get_table("fake_table")


def test_osti_get_schema():
    """Test that get_schema returns informative schema."""
    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE records" in schema
    assert "osti_id" in schema
    assert "title" in schema
    assert "doi" in schema
    assert "has_fulltext" in schema


# =============================================================================
# 4) Find Operations
# =============================================================================


def test_osti_find():
    """Test general find operation across all levels."""
    results = backend.find("title")
    assert isinstance(results, list)


def test_osti_find_table():
    """Test finding tables by name."""
    tables_found = backend.find_table("records")

    assert isinstance(tables_found, list)
    assert len(tables_found) > 0
    assert any("records" in t.t_name for t in tables_found)


def test_osti_find_column():
    """Test finding columns by name."""
    columns_found = backend.find_column("title")

    assert isinstance(columns_found, list)
    assert len(columns_found) > 0
    assert any("title" in c.c_name for c in columns_found)


def test_osti_find_cell():
    """Test finding cells by value."""
    cells_found = backend.find_cell("FIRETEC")

    assert isinstance(cells_found, list)
    assert len(cells_found) > 0
    assert all(hasattr(cell, "type") and cell.type == "cell" for cell in cells_found)


def test_osti_find_relation():
    """Test relation finding against the loaded FIRETEC records."""
    results = backend.find_relation("title", "~FIRETEC")

    assert isinstance(results, list)
    assert len(results) > 0
    assert all(result.type == "relation" for result in results)


# =============================================================================
# 5) URL Validation
# =============================================================================


def test_osti_validate_urls():
    """Test URL validation without making additional external requests."""
    local = OSTI.__new__(OSTI)
    local._loaded = True
    local.verify_ssl = backend.verify_ssl
    local._cache = deepcopy(backend._cache)

    response = SimpleNamespace(status_code=200)

    with patch("dsi.backends.osti.requests.head", return_value=response), \
         patch("dsi.backends.osti.requests.get") as get_mock:
        local.validate_urls()

    table = local._cache["records"]

    for field in [
        "citation_url",
        "citation_doe_pages_url",
        "fulltext_url",
    ]:
        valid_field = f"{field}_valid"
        if field in table:
            assert valid_field in table
            assert all(isinstance(v, bool) for v in table[valid_field])
            assert len(table[valid_field]) == len(table[field])

    get_mock.assert_not_called()


# =============================================================================
# 6) List and Summary
# =============================================================================


def test_osti_list():
    """Test list method returns table names."""
    table_names = backend.list(collection=True)

    assert "records" in table_names
    assert isinstance(table_names, list)


def test_osti_num_tables():
    """Test num_tables does not raise exception."""
    backend.num_tables()


def test_osti_summary():
    """Test summary returns table metadata."""
    summary_list = backend.summary()

    assert isinstance(summary_list, list)
    assert len(summary_list) == 2
    assert isinstance(summary_list[0], list)
    assert "records" in summary_list[0]
    assert isinstance(summary_list[1], pd.DataFrame)

    summary_single = backend.summary("records")

    assert isinstance(summary_single, pd.DataFrame)
    assert "column" in summary_single.columns
    assert "osti_id" in summary_single["column"].tolist()


def test_osti_summary_invalid_table():
    """Test summary rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.summary("fake_table")


def test_osti_display():
    """Test display method for records table."""
    result = backend.display("records", num_rows=10)

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10


def test_osti_display_cols():
    """Test display with selected columns."""
    result = backend.display(
        "records",
        num_rows=5,
        display_cols=["osti_id", "title"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["osti_id", "title"]


def test_osti_display_invalid_table():
    """Test display rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.display("fake_table")


# =============================================================================
# 7) Process Artifacts
# =============================================================================


def test_osti_process_artifacts():
    """Test that processed artifacts have correct structure."""
    artifacts = backend.process_artifacts()

    assert isinstance(artifacts, OrderedDict)
    assert "records" in artifacts
    assert isinstance(artifacts["records"], OrderedDict)


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================


def test_osti_ingest_artifacts():
    """Test that ingest_artifacts raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


# =============================================================================
# 9) Filtering Tests
# =============================================================================


def test_osti_author_filter():
    """Test author filter is included in the combined query."""
    params = backend._build_request_params({"author": "Linn", "rows": 5})
    assert params["author"] == "Linn"


def test_osti_title_filter():
    """Test title filter is included in the combined query."""
    params = backend._build_request_params({"title": "FIRETEC", "rows": 5})
    assert params["title"] == "FIRETEC"


def test_osti_subject_filter():
    """Test subject filter is included in the combined query."""
    params = backend._build_request_params(
        {"subject": "54 ENVIRONMENTAL SCIENCES", "rows": 5}
    )
    assert params["subject"] == "54 ENVIRONMENTAL SCIENCES"


def test_osti_fulltext_filter():
    """Test has_fulltext filter is included in the combined query."""
    params = backend._build_request_params({"has_fulltext": "true", "rows": 5})
    assert params["has_fulltext"] == "true"


def test_osti_combined_filters():
    """Test all requested filters can be stacked into one OSTI request."""
    params = backend._build_request_params(
        {
            "q": "wildfire",
            "author": "Linn",
            "title": "FIRETEC",
            "subject": "54 ENVIRONMENTAL SCIENCES",
            "has_fulltext": "true",
            "rows": 5,
            "page": 1,
        }
    )

    assert params == {
        "q": "wildfire",
        "author": "Linn",
        "title": "FIRETEC",
        "subject": "54 ENVIRONMENTAL SCIENCES",
        "has_fulltext": "true",
        "rows": 5,
        "page": 1,
    }


# =============================================================================
# 10) Lifecycle
# =============================================================================


def test_osti_notebook():
    """Test that notebook() throws NotImplementedError."""
    try:
        backend.notebook()
        assert False
    except NotImplementedError: # should throw error
        assert True


def test_osti_close():
    """Test close() without destroying the shared backend used by other tests."""
    local = OSTI.__new__(OSTI)
    local._loaded = True
    local._cache = OrderedDict(
        {"records": OrderedDict({"osti_id": ["563175"]})}
    )

    local.close()

    assert local._loaded is False
    assert len(local._cache) == 0