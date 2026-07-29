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

pytestmark = pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")

@pytest.fixture
def backend(tmp_path):
    """Open one minimal Oceans11 backend for each test and always close it."""
    instance = Oceans11(
        params={"q": "heat", "rows": 5},
        workspace=str(tmp_path),
    )
    yield instance
    if instance._loaded:
        instance.close()


# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================

def test_oceans11_initialization(backend):
    """Test Oceans11 backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "records" in backend._cache


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

def test_oceans11_load_initial_data(backend):
    """Test that initial data load creates proper structure."""
    assert isinstance(backend._cache, OrderedDict)
    assert "records" in backend._cache
    assert isinstance(backend._cache["records"], OrderedDict)

    record_cols = list(backend._cache["records"].keys())

    assert "osti_id" in record_cols
    assert "title" in record_cols
    assert "authors" in record_cols
    assert "report_number" in record_cols
    assert "t2db_url" in record_cols


def test_oceans11_multiple_query_params(tmp_path):
    """Test loading records from multiple Oceans11 queries."""
    backend = Oceans11(
        params=[
            {"q": "heat", "rows": 3},
            {"q": "wildfire", "rows": 3},
        ],
        workspace=str(tmp_path),
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
        Oceans11(
            params={"bad_param": "heat"},
            workspace=str(tmp_path),
        )


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_oceans11_query_artifacts(backend):
    """Test querying loaded Oceans11 data with pandas query string."""
    result = backend.query_artifacts("title.notnull()", dict_return=True)

    assert isinstance(result, dict)
    assert "records" in result

    for table_name, table_data in result.items():
        assert isinstance(table_data, dict)
        for col_values in table_data.values():
            assert isinstance(col_values, list)


def test_oceans11_query_artifacts_dataframe(backend):
    """Test querying loaded Oceans11 data and returning DataFrames."""
    result = backend.query_artifacts("title.notnull()", dict_return=False)

    assert isinstance(result, dict)
    assert "records" in result
    assert isinstance(result["records"], pd.DataFrame)
    assert not result["records"].empty


def test_oceans11_query_specific_table(backend):
    """Test querying a specific table."""
    result = backend.query_artifacts(
        "title.notnull()",
        table_name="records",
        dict_return=True
    )

    assert isinstance(result, dict)
    assert "records" in result


def test_oceans11_query_invalid(backend):
    """Test that invalid pandas queries raise ValueError."""
    with pytest.raises(ValueError):
        backend.query_artifacts("INVALID SYNTAX ###")


def test_oceans11_query_no_results(backend):
    """Test that queries with no matches raise ValueError."""
    with pytest.raises(ValueError):
        backend.query_artifacts("title == 'not_a_real_title_12345'")


def test_oceans11_get_table(backend):
    """Test getting table data as DataFrame or OrderedDict."""
    df = backend.get_table("records", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    dict_data = backend.get_table("records", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0


def test_oceans11_get_table_invalid_name(backend):
    """Test that requesting a nonexistent table raises ValueError."""
    with pytest.raises(ValueError):
        backend.get_table("fake_table")


def test_oceans11_get_schema(backend):
    """Test that get_schema returns informative schema."""
    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE records" in schema
    assert "osti_id" in schema
    assert "title" in schema
    assert "doi" in schema
    assert "t2db_url" in schema


def test_oceans11_get_table_names(backend):
    """Test extracting table names from query strings."""
    names = backend.get_table_names("SELECT * FROM records WHERE title LIKE '%heat%'")

    assert isinstance(names, list)
    assert "records" in names

# =============================================================================
# 4) Find Operations
# =============================================================================

def test_oceans11_find(backend):
    """Test general find operation across all levels."""
    results = backend.find("title")

    assert isinstance(results, list)


def test_oceans11_find_table(backend):
    """Test finding tables by name."""
    tables_found = backend.find_table("records")

    assert isinstance(tables_found, list)
    assert len(tables_found) > 0
    assert any("records" in t.t_name for t in tables_found)


def test_oceans11_find_column(backend):
    """Test finding columns by name."""
    columns_found = backend.find_column("title")

    assert isinstance(columns_found, list)
    assert len(columns_found) > 0
    assert any("title" in c.c_name for c in columns_found)


def test_oceans11_find_cell(backend):
    """Test finding cells by value."""
    cells_found = backend.find_cell("climate")

    assert isinstance(cells_found, list)

    if cells_found:
        assert all(hasattr(cell, "type") and cell.type == "cell" for cell in cells_found)
        assert all(hasattr(cell, "t_name") for cell in cells_found)
        assert all(hasattr(cell, "c_name") for cell in cells_found)
        assert all(hasattr(cell, "row_num") for cell in cells_found)
        assert all(hasattr(cell, "value") for cell in cells_found)


def test_oceans11_find_relation(backend):
    """Test finding rows by column relation."""
    results = backend.find_relation("title", "!= ''")

    assert isinstance(results, list)

    if results:
        assert all(hasattr(row, "type") and row.type == "relation" for row in results)
        assert all(hasattr(row, "t_name") for row in results)
        assert all(hasattr(row, "c_name") for row in results)
        assert all(hasattr(row, "row_num") for row in results)
        assert all(hasattr(row, "value") for row in results)

# =============================================================================
# 5) URL Validation - Not relevant to Oceans11
# =============================================================================


# =============================================================================
# 6) List and Summary
# =============================================================================

def test_oceans11_list(backend):
    """Test list method returns table names."""
    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "records" in table_names


def test_oceans11_num_tables(backend):
    """Test num_tables does not raise exception."""
    backend.num_tables()


def test_oceans11_summary(backend):
    """Test summary returns table metadata."""
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


def test_oceans11_summary_invalid_table(backend):
    """Test summary rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.summary("fake_table")


def test_oceans11_display(backend):
    """Test display method for records table."""
    result = backend.display("records", num_rows=10)

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10


def test_oceans11_display_cols(backend):
    """Test display with selected columns."""
    result = backend.display(
        "records",
        num_rows=5,
        display_cols=["osti_id", "title"]
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["osti_id", "title"]


def test_oceans11_display_invalid_table(backend):
    """Test display rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.display("fake_table")

# =============================================================================
# 7) Process Artifacts
# =============================================================================

def test_oceans11_process_artifacts(backend):
    """Test that processed artifacts have correct structure."""
    artifacts = backend.process_artifacts()

    assert isinstance(artifacts, OrderedDict)
    assert "records" in artifacts
    assert isinstance(artifacts["records"], OrderedDict)

# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_oceans11_ingest_artifacts(backend):
    """Test that ingest_artifacts raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})

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
        params={"authors": "Debardeleben", "rows": 4},
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

@pytest.mark.integration
@pytest.mark.slow
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

def test_oceans11_close(backend):
    """Test that close() properly resets backend state."""
    assert backend._loaded is True
    assert len(backend._cache) > 0

    backend.close()

    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._resource_tables) == 0
    assert len(backend._dataset_id_map) == 0
    assert len(backend._dataset_title_map) == 0


def test_oceans11_notebook(backend):
    """Test that notebook() doesn't raise errors."""
    backend.notebook()