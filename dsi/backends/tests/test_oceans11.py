"""
Oceans11 Backend Function Tests

Tests Oceans11 backend methods directly without Terminal integration.

The live Oceans11 backend is initialized once for the entire test session to
avoid repeatedly downloading the Oceans11 catalog and Tier-2 databases.
Tier-1 search behavior is tested against the already-downloaded local catalog.
"""

from collections import OrderedDict
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from dsi.backends.oceans11 import Oceans11


pytestmark = pytest.mark.filterwarnings(
    "ignore::urllib3.exceptions.InsecureRequestWarning"
)


# A known, small Oceans11 dataset that is expected to remain in the catalog.
# Using the same record twice lets the shared initialization also exercise
# list-of-dict params and Tier-1 deduplication while downloading only one
# Tier-2 database.
TEST_OSTI_ID = "3022783"


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def backend(tmp_path_factory):
    """Create one live Oceans11 backend for the entire test session."""
    workspace = tmp_path_factory.mktemp("oceans11")

    instance = Oceans11(
        params=[
            {"osti_id": TEST_OSTI_ID, "rows": 1},
            {"osti_id": TEST_OSTI_ID, "rows": 1},
        ],
        workspace=str(workspace),
    )

    yield instance

    if instance._loaded:
        instance.close()


@pytest.fixture(scope="session")
def record(backend):
    """Return the Tier-1 record loaded by the shared backend."""
    df = backend.get_table("records", dict_return=False)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    return df.iloc[0]


# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================

def test_oceans11_initialization(backend):
    """Test Oceans11 initializes and loads expected Tier-1 data."""
    assert backend._loaded is True
    assert isinstance(backend._cache, OrderedDict)
    assert len(backend._cache) > 0

    assert "records" in backend._cache
    assert "filesystem" in backend._cache


def test_oceans11_catalog_downloaded(backend):
    """Test initialization downloaded a usable Tier-1 catalog."""
    assert isinstance(backend.catalog_path, str)
    assert Path(backend.catalog_path).is_file()


def test_oceans11_invalid_url():
    """Test invalid URLs fail before any network access."""
    with pytest.raises(ValueError):
        Oceans11(
            url="not-a-valid-url",
            only_validate=True,
        )


# =============================================================================
# 2) Data Loading and Structure
# =============================================================================

def test_oceans11_load_initial_data(backend):
    """Test initial data loading creates the expected table structure."""
    assert isinstance(backend._cache["records"], OrderedDict)
    assert isinstance(backend._cache["filesystem"], OrderedDict)

    record_cols = list(backend._cache["records"].keys())

    expected_columns = {
        "osti_id",
        "title",
        "authors",
        "subjects",
        "report_number",
        "t2db_url",
        "t2db_path",
        "t2db_name",
    }

    assert expected_columns.issubset(record_cols)


def test_oceans11_multiple_query_params_are_deduplicated(backend):
    """Test list-of-dict initialization merges duplicate Tier-1 records."""
    records = backend.get_table("records", dict_return=False)

    assert len(records) == 1
    assert records["osti_id"].nunique() == 1
    assert str(records.iloc[0]["osti_id"]) == TEST_OSTI_ID


def test_oceans11_tier2_tables_loaded(backend):
    """Test the selected record loads at least one Tier-2 table."""
    assert isinstance(backend._resource_tables, list)
    assert len(backend._resource_tables) > 0

    for table_name in backend._resource_tables:
        assert table_name in backend._cache


def test_oceans11_bad_params_type(backend):
    """Test invalid initial-data parameter types are rejected."""
    with pytest.raises(TypeError):
        backend._load_initial_data("not-a-dict-or-list")


def test_oceans11_unsupported_param(backend):
    """Test unsupported Tier-1 query parameters are rejected."""
    with pytest.raises(ValueError):
        backend._run_single_query({"bad_param": "heat"})


def test_oceans11_invalid_rows(backend):
    """Test rows must be greater than zero."""
    with pytest.raises(ValueError):
        backend._run_single_query({"q": "heat", "rows": 0})


# =============================================================================
# 3) Query / Table Operations
# =============================================================================

def test_oceans11_query_artifacts_not_supported(backend):
    """Oceans11 does not expose the public SQL query interface."""
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("SELECT * FROM records")


def test_oceans11_get_table(backend):
    """Test table retrieval as DataFrame and OrderedDict."""
    df = backend.get_table("records", dict_return=False)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    dict_data = backend.get_table("records", dict_return=True)

    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0


def test_oceans11_get_table_invalid_name(backend):
    """Test requesting a nonexistent table raises ValueError."""
    with pytest.raises(ValueError):
        backend.get_table("fake_table")


def test_oceans11_get_tier2_table(backend):
    """Test a loaded Tier-2 table can be retrieved directly."""
    table_name = backend._resource_tables[0]
    result = backend.get_table(table_name, dict_return=False)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_oceans11_get_schema(backend):
    """Test get_schema returns informative CREATE TABLE-style text."""
    schema = backend.get_schema()

    assert isinstance(schema, str)
    assert "CREATE TABLE records" in schema
    assert "osti_id" in schema
    assert "title" in schema
    assert "doi" in schema
    assert "t2db_url" in schema


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_oceans11_find(backend):
    """Test general find across table, column, and cell levels."""
    results = backend.find("title")

    assert isinstance(results, list)
    assert len(results) > 0


def test_oceans11_find_table(backend):
    """Test finding cached tables by name."""
    results = backend.find_table("records")

    assert isinstance(results, list)
    assert len(results) > 0
    assert any(result.t_name == "records" for result in results)


def test_oceans11_find_column(backend):
    """Test finding cached columns by name."""
    results = backend.find_column("title")

    assert isinstance(results, list)
    assert len(results) > 0
    assert any("title" in result.c_name for result in results)


def test_oceans11_find_cell(backend, record):
    """Test finding an exact cell value."""
    osti_id = str(record["osti_id"])
    results = backend.find_cell(osti_id)

    assert isinstance(results, list)
    assert len(results) > 0

    assert all(result.type == "cell" for result in results)
    assert all(hasattr(result, "t_name") for result in results)
    assert all(hasattr(result, "c_name") for result in results)
    assert all(hasattr(result, "row_num") for result in results)
    assert all(hasattr(result, "value") for result in results)


def test_oceans11_find_cell_rows_are_unique(backend, record):
    """Test row-mode search returns each matching row only once."""
    subjects = record.get("subjects")

    if subjects is None or not str(subjects).strip():
        pytest.skip("Selected Oceans11 record has no subjects value")

    # Use one current subject token from the loaded record rather than
    # depending on a specific Tier-2 table name or total table count.
    query = str(subjects).split(";")[0].strip()

    results = backend.find_cell(query, row=True)

    assert isinstance(results, list)

    row_keys = [
        (result.t_name, result.row_num)
        for result in results
    ]

    assert len(row_keys) == len(set(row_keys))
    assert all(result.type == "row" for result in results)


def test_oceans11_find_relation(backend, record):
    """Test finding rows using a column relation."""
    osti_id = str(record["osti_id"])

    results = backend.find_relation(
        "osti_id",
        f"== '{osti_id}'",
    )

    assert isinstance(results, list)
    assert len(results) > 0

    assert all(result.type == "relation" for result in results)
    assert all(result.t_name == "records" for result in results)


# =============================================================================
# 5) Tier-1 Search Operations
# =============================================================================

def test_oceans11_exact_id_filter(backend, record):
    """Test exact OSTI-ID filtering against the local catalog."""
    osti_id = str(record["osti_id"])

    results = backend._run_single_query(
        {
            "osti_id": osti_id,
            "rows": 1,
        }
    )

    assert isinstance(results, list)
    assert len(results) == 1
    assert str(results[0]["osti_id"]) == osti_id


def test_oceans11_title_filter(backend, record):
    """Test title filtering against the local Tier-1 catalog."""
    title = str(record["title"])

    results = backend._run_single_query(
        {
            "title": title,
            "rows": 5,
        }
    )

    assert isinstance(results, list)
    assert len(results) > 0

    assert any(
        str(result["osti_id"]) == str(record["osti_id"])
        for result in results
    )


def test_oceans11_keyword_filter(backend, record):
    """Test keyword filtering against the local Tier-1 catalog."""
    report_number = record.get("report_number")

    if report_number is None or not str(report_number).strip():
        pytest.skip("Selected Oceans11 record has no report_number value")

    results = backend._run_single_query(
        {
            "keyword": str(report_number),
            "rows": 5,
        }
    )

    assert isinstance(results, list)
    assert len(results) > 0

    assert any(
        str(result["osti_id"]) == str(record["osti_id"])
        for result in results
    )


def test_oceans11_subject_filter(backend, record):
    """Test subject filtering against the local Tier-1 catalog."""
    subjects = record.get("subjects")

    if subjects is None or not str(subjects).strip():
        pytest.skip("Selected Oceans11 record has no subjects value")

    subject = str(subjects).split(";")[0].strip()

    results = backend._run_single_query(
        {
            "subject": subject,
            "rows": 5,
        }
    )

    assert isinstance(results, list)
    assert len(results) > 0

    assert any(
        str(result["osti_id"]) == str(record["osti_id"])
        for result in results
    )


def test_oceans11_all_records_query(backend):
    """
    Test the Tier-1 operation used by download_all.

    This intentionally does not call Oceans11(download_all=True), which would
    download every associated Tier-2 database and make CI unnecessarily slow.
    """
    records = backend._run_all_records_query()

    assert isinstance(records, list)
    assert len(records) > 0
    assert all(isinstance(item, dict) for item in records)
    assert all("osti_id" in item for item in records)


# =============================================================================
# 6) List and Summary
# =============================================================================

def test_oceans11_list(backend):
    """Test list returns all currently cached table names."""
    table_names = backend.list(collection=True)

    assert isinstance(table_names, list)
    assert "records" in table_names
    assert "filesystem" in table_names

    for table_name in backend._resource_tables:
        assert table_name in table_names


def test_oceans11_num_tables(backend, capsys):
    """Test num_tables reports the current cache size."""
    backend.num_tables()

    output = capsys.readouterr().out

    assert str(len(backend._cache)) in output


def test_oceans11_summary(backend):
    """Test summary returns current column-level metadata."""
    result = backend.summary()

    assert isinstance(result, list)
    assert len(result) >= 2

    table_names = result[0]

    assert isinstance(table_names, list)
    assert "records" in table_names
    assert "filesystem" in table_names

    expected_columns = [
        "column",
        "type",
        "unique",
        "min",
        "max",
        "avg",
        "std_dev",
    ]

    for summary_df in result[1:]:
        assert isinstance(summary_df, pd.DataFrame)
        assert list(summary_df.columns) == expected_columns


def test_oceans11_summary_single_table(backend):
    """Test summary for one table."""
    summary = backend.summary("records")

    assert isinstance(summary, pd.DataFrame)

    assert list(summary.columns) == [
        "column",
        "type",
        "unique",
        "min",
        "max",
        "avg",
        "std_dev",
    ]

    assert "osti_id" in summary["column"].tolist()
    assert "title" in summary["column"].tolist()
    assert "t2db_path" in summary["column"].tolist()


def test_oceans11_summary_invalid_table(backend):
    """Test summary rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.summary("fake_table")


# =============================================================================
# 7) Display
# =============================================================================

def test_oceans11_display(backend):
    """Test display for the records table."""
    result = backend.display(
        "records",
        num_rows=10,
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10


def test_oceans11_display_cols(backend):
    """Test display with selected columns."""
    result = backend.display(
        "records",
        num_rows=5,
        display_cols=["osti_id", "title"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["osti_id", "title"]


def test_oceans11_display_invalid_table(backend):
    """Test display rejects invalid table names."""
    with pytest.raises(ValueError):
        backend.display("fake_table")


# =============================================================================
# 8) Process Artifacts
# =============================================================================

def test_oceans11_process_artifacts(backend):
    """Test all cached Oceans11 tables are exposed for processing."""
    artifacts = backend.process_artifacts()

    assert isinstance(artifacts, OrderedDict)
    assert "records" in artifacts
    assert "filesystem" in artifacts

    assert isinstance(artifacts["records"], OrderedDict)
    assert isinstance(artifacts["filesystem"], OrderedDict)

    for table_name in backend._resource_tables:
        assert table_name in artifacts


# =============================================================================
# 9) Read-Only Enforcement
# =============================================================================

def test_oceans11_ingest_artifacts(backend):
    """Test Oceans11 cannot ingest/write data."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


# =============================================================================
# 10) Utility Helpers
# =============================================================================

def test_oceans11_rows_to_table(backend):
    """Test conversion from row dictionaries to column-oriented data."""
    rows = [
        {
            "id": 1,
            "name": "one",
        },
        {
            "id": 2,
            "name": "two",
            "extra": "value",
        },
    ]

    result = backend._rows_to_table(rows)

    assert isinstance(result, OrderedDict)
    assert result["id"] == [1, 2]
    assert result["name"] == ["one", "two"]
    assert result["extra"] == [None, "value"]


def test_oceans11_deduplicate_records(backend):
    """Test Tier-1 record deduplication."""
    records = [
        {
            "osti_id": "1",
            "title": "one",
        },
        {
            "osti_id": "1",
            "title": "duplicate",
        },
        {
            "osti_id": "2",
            "title": "two",
        },
    ]

    result = backend._deduplicate_records(records)

    assert len(result) == 2
    assert result[0]["title"] == "one"
    assert result[1]["title"] == "two"


def test_oceans11_escape_sql(backend):
    """Test SQL literal escaping."""
    assert backend._escape_sql("O'Brien") == "O''Brien"


def test_oceans11_parse_relation(backend):
    """Test relation parsing."""
    assert backend._parse_relation(">= 10") == (">=", 10)
    assert backend._parse_relation("<= 20") == ("<=", 20)
    assert backend._parse_relation("== 'heat'") == ("==", "heat")
    assert backend._parse_relation("~~ heat") == ("contains", "heat")
    assert backend._parse_relation("(1, 5)") == (
        "range",
        (1, 5),
    )


# =============================================================================
# 11) Lifecycle
# =============================================================================

def test_oceans11_close_without_network():
    """Test close on a normally initialized backend without network access."""
    with patch.object(
        Oceans11,
        "validate_connection",
        return_value="/tmp/fake-oceans11.db",
    ):
        instance = Oceans11()

    instance._cache = OrderedDict(
        {
            "records": OrderedDict(
                {
                    "osti_id": ["1"],
                }
            )
        }
    )

    instance._tier1_tables = ["records"]
    instance._resource_tables = ["example_files"]

    instance._dataset_id_map = {
        "1": {
            "osti_id": "1",
        }
    }

    instance._dataset_title_map = {
        "Example": {
            "osti_id": "1",
        }
    }

    instance._dataset_table_map = {
        "1": ["example_files"],
    }

    instance.catalog_path = "/tmp/fake-oceans11.db"
    instance._loaded = True

    instance.close()

    assert instance._loaded is False
    assert instance.catalog_path is None

    assert len(instance._cache) == 0
    assert len(instance._tier1_tables) == 0
    assert len(instance._resource_tables) == 0
    assert len(instance._dataset_id_map) == 0
    assert len(instance._dataset_title_map) == 0
    assert len(instance._dataset_table_map) == 0


def test_oceans11_notebook(backend):
    """Test notebook is currently a no-op."""
    try:
        backend.notebook()
        assert False
    except NotImplementedError: # should throw error
        assert True