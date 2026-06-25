"""
Oceans11 Backend Function Tests

Tests Oceans11 backend methods directly without Terminal integration.
Mirrors structure of test_sqlite.py
"""

import pytest
import pandas as pd
from pathlib import Path
from collections import OrderedDict
from dsi.backends.oceans11 import Oceans11


# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================

def test_oceans11_initialization(tmp_path):
    """Test Oceans11 backend initializes correctly."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "records" in backend._cache

    backend.close()


def test_oceans11_validate_connection(tmp_path):
    """Test connection validation to Oceans11 catalog."""
    backend = Oceans11(workspace=str(tmp_path))

    catalog_path = backend.validate_connection()

    assert isinstance(catalog_path, str)
    assert Path(catalog_path).is_file()

    backend.close()


def test_oceans11_invalid_url():
    """Test that invalid URL raises appropriate error."""
    with pytest.raises(ValueError):
        Oceans11(url="not-a-valid-url")


# =============================================================================
# 2) Data Loading and Structure
# =============================================================================

def test_oceans11_load_initial_data(tmp_path):
    """Test that initial data load creates proper structure."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    assert isinstance(backend._cache, OrderedDict)
    assert "records" in backend._cache
    assert isinstance(backend._cache["records"], OrderedDict)

    record_cols = list(backend._cache["records"].keys())

    assert "osti_id" in record_cols
    assert "title" in record_cols
    assert "authors" in record_cols
    assert "report_number" in record_cols
    assert "t2db_url" in record_cols

    backend.close()


def test_oceans11_multiple_query_params(tmp_path):
    """Test loading records from multiple Oceans11 queries."""
    backend = Oceans11(
        params=[
            {"q": "heat", "rows": 3},
            {"q": "wildfire", "rows": 3}
        ],
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    assert "records" in backend._cache
    assert isinstance(backend._cache["records"], OrderedDict)

    backend.close()


def test_oceans11_bad_params_type(tmp_path):
    """Test that invalid params type raises RuntimeError from initialization."""
    with pytest.raises(RuntimeError):
        Oceans11(params="not-a-dict-or-list", workspace=str(tmp_path))


def test_oceans11_unsupported_param(tmp_path):
    """Test that unsupported query parameters raise RuntimeError."""
    with pytest.raises(RuntimeError):
        Oceans11(params={"bad_param": "heat"},
        workspace=str(tmp_path)
        )

# =============================================================================
# 3) Query Operations
# =============================================================================

def test_oceans11_query_artifacts(tmp_path):
    """Test querying loaded Oceans11 data with pandas query string."""
    backend = Oceans11(
        params={"q": "heat", "rows": 4},
        workspace=str(tmp_path)
    )

    result = backend.query_artifacts("title.notnull()", dict_return=True)

    assert isinstance(result, dict)
    assert "records" in result

    for table_name, table_data in result.items():
        assert isinstance(table_data, dict)
        for col_values in table_data.values():
            assert isinstance(col_values, list)

    backend.close()


def test_oceans11_query_artifacts_dataframe(tmp_path):
    """Test querying loaded Oceans11 data and returning DataFrames."""
    backend = Oceans11(
        params={"q": "heat", "rows": 10},
        workspace=str(tmp_path)
    )

    result = backend.query_artifacts("title.notnull()", dict_return=False)

    assert isinstance(result, dict)
    assert "records" in result
    assert isinstance(result["records"], pd.DataFrame)
    assert not result["records"].empty

    backend.close()


def test_oceans11_query_specific_table(tmp_path):
    """Test querying a specific table."""
    backend = Oceans11(
        params={"q": "heat", "rows": 10},
        workspace=str(tmp_path)
    )

    result = backend.query_artifacts(
        "title.notnull()",
        table_name="records",
        dict_return=True
    )

    assert isinstance(result, dict)
    assert "records" in result

    backend.close()


def test_oceans11_query_invalid(tmp_path):
    """Test that invalid pandas queries raise ValueError."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    with pytest.raises(ValueError):
        backend.query_artifacts("INVALID SYNTAX ###")

    backend.close()


def test_oceans11_query_no_results(tmp_path):
    """Test that queries with no matches raise ValueError."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    with pytest.raises(ValueError):
        backend.query_artifacts("title == 'not_a_real_title_12345'")

    backend.close()


def test_oceans11_get_table(tmp_path):
    """Test getting table data as DataFrame or OrderedDict."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    df = backend.get_table("records", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    dict_data = backend.get_table("records", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0

    backend.close()


def test_oceans11_get_table_invalid_name(tmp_path):
    """Test that requesting a nonexistent table raises ValueError."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    with pytest.raises(ValueError):
        backend.get_table("fake_table")

    backend.close()


def test_oceans11_get_schema(tmp_path):
    """Test that get_schema returns informative schema."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE records" in schema
    assert "osti_id" in schema
    assert "title" in schema
    assert "doi" in schema
    assert "t2db_url" in schema

    backend.close()


def test_oceans11_get_table_names(tmp_path):
    """Test extracting table names from query strings."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    names = backend.get_table_names("SELECT * FROM records WHERE title LIKE '%heat%'")

    assert isinstance(names, list)
    assert "records" in names

    backend.close()


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_oceans11_find(tmp_path):
    """Test general find operation across all levels."""
    backend = Oceans11(
        params={"q": "heat", "rows": 8},
        workspace=str(tmp_path)
    )

    results = backend.find("title")

    assert isinstance(results, list)

    backend.close()

def test_oceans11_find_table(tmp_path):
    """Test finding tables by name."""
    backend = Oceans11(
        params={"q": "heat", "rows": 8},
        workspace=str(tmp_path)
    )

    tables_found = backend.find_table("records")

    assert isinstance(tables_found, list)
    assert len(tables_found) > 0
    assert any("records" in t.t_name for t in tables_found)

    backend.close()

def test_oceans11_find_column(tmp_path):
    """Test finding columns by name."""
    backend = Oceans11(
        params={"q": "heat", "rows": 8},
        workspace=str(tmp_path)
    )

    columns_found = backend.find_column("title")

    assert isinstance(columns_found, list)
    assert len(columns_found) > 0
    assert any("title" in c.c_name for c in columns_found)

    backend.close()

def test_oceans11_find_cell(tmp_path):
    """Test finding cells by value."""
    backend = Oceans11(
        params={"q": "heat", "rows": 8},
        workspace=str(tmp_path)
    )

    cells_found = backend.find_cell("climate")

    assert isinstance(cells_found, list)

    if cells_found:
        assert all(hasattr(cell, "type") and cell.type == "cell" for cell in cells_found)
        assert all(hasattr(cell, "t_name") for cell in cells_found)
        assert all(hasattr(cell, "c_name") for cell in cells_found)
        assert all(hasattr(cell, "row_num") for cell in cells_found)
        assert all(hasattr(cell, "value") for cell in cells_found)

    backend.close()

def test_oceans11_find_relation(tmp_path):
    """Test finding rows by column relation."""
    backend = Oceans11(
        params={"q": "heat", "rows": 8},
        workspace=str(tmp_path)
    )

    results = backend.find_relation("title", "!= ''")

    assert isinstance(results, list)

    if results:
        assert all(hasattr(row, "type") and row.type == "relation" for row in results)
        assert all(hasattr(row, "t_name") for row in results)
        assert all(hasattr(row, "c_name") for row in results)
        assert all(hasattr(row, "row_num") for row in results)
        assert all(hasattr(row, "value") for row in results)

    backend.close()

# =============================================================================
# 5) URL Validation - Not relevant to Oceans11
# =============================================================================

# =============================================================================
# 6) List and Summary
# =============================================================================

def test_oceans11_list(tmp_path):
    """Test list method returns table names."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "records" in table_names

    backend.close()


def test_oceans11_num_tables(tmp_path):
    """Test num_tables does not raise exception."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    backend.num_tables()

    backend.close()


def test_oceans11_summary(tmp_path):
    """Test summary returns table metadata."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    summary_list = backend.summary()

    assert isinstance(summary_list, list)
    assert len(summary_list) >= 2
    assert isinstance(summary_list[0], list)
    assert "records" in summary_list[0]

    for df in summary_list[1:]:
        assert isinstance(df, pd.DataFrame)
        assert "table_name" in df.columns
        assert "num_rows" in df.columns
        assert "num_columns" in df.columns
        assert "columns" in df.columns
        assert "tier" in df.columns

    summary_single = backend.summary("records")

    assert isinstance(summary_single, pd.DataFrame)
    assert "table_name" in summary_single.columns
    assert summary_single.iloc[0]["table_name"] == "records"
    assert summary_single.iloc[0]["tier"] == "T1"

    backend.close()


def test_oceans11_summary_invalid_table(tmp_path):
    """Test summary rejects invalid table names."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    with pytest.raises(ValueError):
        backend.summary("fake_table")

    backend.close()


def test_oceans11_display(tmp_path):
    """Test display method for records table."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    result = backend.display("records", num_rows=10)

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10

    backend.close()


def test_oceans11_display_cols(tmp_path):
    """Test display with selected columns."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    result = backend.display(
        "records",
        num_rows=5,
        display_cols=["osti_id", "title"]
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["osti_id", "title"]

    backend.close()


def test_oceans11_display_invalid_table(tmp_path):
    """Test display rejects invalid table names."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    with pytest.raises(ValueError):
        backend.display("fake_table")

    backend.close()


# =============================================================================
# 7) Process Artifacts
# =============================================================================

def test_oceans11_process_artifacts(tmp_path):
    """Test that processed artifacts have correct structure."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    artifacts = backend.process_artifacts()

    assert isinstance(artifacts, OrderedDict)
    assert "records" in artifacts
    assert isinstance(artifacts["records"], OrderedDict)

    backend.close()


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_oceans11_ingest_artifacts(tmp_path):
    """Test that ingest_artifacts raises NotImplementedError."""
    backend = Oceans11(
        params={"q": "heat", "rows": 3},
        workspace=str(tmp_path)
    )

    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})

    backend.close()

# =============================================================================
# 9) Filtering Tests
# =============================================================================

def test_oceans11_title_filter(tmp_path):
    """Test loading data with title filter."""
    backend = Oceans11(
        params={"title": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)

    backend.close()


def test_oceans11_authors_filter(tmp_path):
    """Test loading data with authors filter."""
    backend = Oceans11(
        params={"authors": "Debardeleben", "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)

    backend.close()


def test_oceans11_keyword_filter(tmp_path):
    """Test loading data with keyword filter."""
    backend = Oceans11(
        params={"keyword": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    data = backend.process_artifacts()

    assert "records" in data
    assert isinstance(data["records"], OrderedDict)

    backend.close()


def test_oceans11_download_all(tmp_path):
    """Test loading all records with download_all."""
    backend = Oceans11(
        params={"download_all": True, "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    assert "records" in backend._cache

    backend.close()


# =============================================================================
# 10) Lifecycle
# =============================================================================

def test_oceans11_close(tmp_path):
    """Test that close() properly resets backend state."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    assert backend._loaded is True
    assert len(backend._cache) > 0

    backend.close()

    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._resource_tables) == 0
    assert len(backend._dataset_id_map) == 0
    assert len(backend._dataset_title_map) == 0


def test_oceans11_notebook(tmp_path):
    """Test that notebook() doesn't raise errors."""
    backend = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path)
    )

    backend.notebook()

    backend.close()