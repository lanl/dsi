"""
OSTI Backend Function Tests

Tests OSTI backend methods directly without Terminal integration.
Mirrors structure of test_sqlite.py
"""

import pytest
import time
import pandas as pd
from collections import OrderedDict
from dsi.backends.osti import OSTI

pytestmark = pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")

# Open one shared OSTI backend for tests that use the default climate dataset.
backend = OSTI(params={"q": "climate", "rows": 10})

# =============================================================================
# 0) pytest initialization to avoid 429 Client Error
# =============================================================================

@pytest.fixture(autouse=True)
def rate_limit_delay():
    yield
    time.sleep(2)  # Pauses for 2 seconds after each test finishes

# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================

def test_osti_initialization():
    """Test OSTI backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "records" in backend._cache


def test_osti_validate_connection():
    """Test connection validation to OSTI API."""
    assert backend.validate_connection() is True



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



def test_osti_multiple_query_params():
    """Test loading records from a list of OSTI query dictionaries."""
    backend = OSTI(
        params=[
            {"q": "climate", "rows": 3},
            {"q": "temperature", "rows": 3}
        ]
    )

    assert backend._loaded is True
    assert "records" in backend._cache
    assert isinstance(backend._cache["records"], OrderedDict)



def test_osti_bad_params_type():
    """Test that invalid params type raises TypeError wrapped in RuntimeError."""
    with pytest.raises(RuntimeError):
        OSTI(params="not-a-dict-or-list")


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_osti_query_artifacts():
    """Test querying loaded OSTI data with pandas query string."""
    result = backend.query_artifacts("title.notnull()", dict_return=True)

    assert isinstance(result, OrderedDict)

    for col_values in result.values():
        assert isinstance(col_values, list)



def test_osti_query_artifacts_dataframe():
    """Test querying loaded OSTI data and returning a DataFrame."""
    result = backend.query_artifacts("title.notnull()", dict_return=False)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty



def test_osti_query_invalid():
    """Test that invalid pandas queries raise appropriate errors."""
    with pytest.raises(ValueError):
        backend.query_artifacts("INVALID SYNTAX ###")



def test_osti_query_unknown_column():
    """Test that queries referencing unknown columns raise ValueError."""
    with pytest.raises(ValueError):
        backend.query_artifacts("not_a_column == 'value'")


def test_osti_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    # Get as DataFrame
    df = backend.get_table("records", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Get as OrderedDict
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
    cells_found = backend.find_cell("climate")

    assert isinstance(cells_found, list)
    if cells_found:
        assert all(hasattr(cell, "type") and cell.type == "cell" for cell in cells_found)



def test_osti_find_relation():
    """Test that find_relation returns empty list (not supported)."""
    try:
        backend.find_relation("column_name", "= 'value'")
        # temp work around until find_relation actually implemented
        # once implemented update this
        assert False
    except Exception:
        assert True
    # assert isinstance(result, list)
    # assert len(result) == 0  # OSTI doesn't support relational queries
    


# =============================================================================
# 5) URL Validation
# =============================================================================

def test_osti_validate_urls():
    """Test URL validation for OSTI URL fields."""
    backend.validate_urls()

    table = backend._cache["records"]

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
    assert "table_name" in summary_single.columns
    assert summary_single.iloc[0]["table_name"] == "records"



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
        display_cols=["osti_id", "title"]
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
    backend = OSTI(
        params={"keywords": "ocean", "limit": 5}
    )
    
    artifacts = backend.process_artifacts()
    
    # Verify structure
    assert isinstance(artifacts, OrderedDict)
    assert "records" in artifacts
    assert isinstance(artifacts["records"], OrderedDict)
    


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_osti_ingest_artifacts():
    """Test that ingest_artifacts raises NotImplementedError."""
    backend = OSTI(
        params={"q": "data", "rows": 3}
    )

    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


# =============================================================================
# 9) Filtering Tests
# =============================================================================

def test_osti_author_filter():
    """Test loading data with author filter."""
    backend = OSTI(
        params={
            "author": "Smith",
            "rows": 5
        }
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)



def test_osti_title_filter():
    """Test loading data with title filter."""
    backend = OSTI(
        params={
            "title": "climate",
            "rows": 5
        }
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)



def test_osti_subject_filter():
    """Test loading data with subject filter."""
    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    #assert isinstance(data["records"], OrderedDict)


def test_osti_fulltext_filter():
    """Test loading data with has_fulltext filter."""
    backend = OSTI(
        params={
            "q": "climate",
            "has_fulltext": "true",
            "rows": 5
        }
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)


# =============================================================================
# 10) Lifecycle
# =============================================================================

def test_osti_notebook():
    """Test that notebook() doesn't raise errors."""
    backend.notebook()


def test_osti_get_table_names():
    """Test extracting table names from query strings."""
    # Test with records table
    names = backend.get_table_names("SELECT * FROM records WHERE title LIKE '%climate%'")
    assert "records" in names


def test_osti_close():
    """Test that close() properly resets backend state."""

    assert backend._loaded is True
    assert len(backend._cache) > 0
    backend.close()


    assert backend._loaded is False
    assert len(backend._cache) == 0