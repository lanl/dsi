"""
NAVDAT Backend Function Tests

Essential tests for NAVDAT backend core functionality, against the live
PetDB v4 API (https://api.earthchem.org).

Remote-server calls are minimized by sharing NAVDAT instances through
module-scoped pytest fixtures. Tests that close or otherwise invalidate
an instance use dedicated fixtures.

NOTE: `authors` is confirmed (2026-08-24) to filter BOTH GET /v4/citations
and GET /v4/locations/samples (totalCount 149778 -> 1558 with
authors=Walker). See navdat.py's module docstring for the full record of
what's confirmed vs. still open.

ENVIRONMENT NOTE: the truststore injection below is required on machines
behind a corporate TLS-inspecting proxy (e.g. Zscaler) - see navdat.py's
module docstring for the full diagnosis. It's applied here, scoped to the
test suite, rather than inside navdat.py itself, because it changes SSL
trust behavior for the whole Python process - that's reasonable to impose
on "running this test suite" but not on "importing this backend module" in
someone else's application.
"""

# Must run before any `requests`/`ssl` usage in this process. See the
# ENVIRONMENT NOTE above and navdat.py's module docstring for why this is
# needed and how it was confirmed (not just assumed) to be necessary.
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass  # fine on machines that don't need OS-trust-store bridging

from collections import OrderedDict

import pandas as pd
import pytest

from dsi.backends.navdat import NAVDAT

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def backend():
    """
    Shared backend for non-destructive tests.
    Do not call close() on this fixture from an individual test.
    """
    instance = NAVDAT(
        params={
            "authors": "Walker",
            "size": 5,
        }
    )
    yield instance
    instance.close()


@pytest.fixture(scope="module")
def filtered_backend():
    """
    Shared backend for filter and multiple-query tests.
    """
    instance = NAVDAT(
        params=[
            {"authors": "Hekinian", "size": 5},
            {"sampleNames": "(2.151) 12.40-12.85", "size": 5},
        ]
    )
    yield instance
    instance.close()


@pytest.fixture(scope="module")
def empty_backend():
    """Shared backend for tests requiring an empty query result."""
    instance = NAVDAT(
        params={
            "sampleNames": "zzzzznonexistentsamplenamezzzzz",
            "size": 5,
        }
    )
    yield instance
    instance.close()


# =============================================================================
# 1) Initialization & Connection
# =============================================================================

def test_navdat_initialization(backend):
    """Test NAVDAT backend initializes correctly."""
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "samples" in backend._cache


def test_navdat_validate_connection(backend):
    """Test connection validation to the PetDB v4 API."""
    assert backend.validate_connection() is True


# =============================================================================
# 2) Data Loading & Structure
# =============================================================================

def test_navdat_samples_table_present(backend):
    """Test that the samples table is populated with rows."""
    df = backend.get_table("samples", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # confirmed columns from the flattened /v4/locations/samples response
    for expected_col in ("sampleName", "sampleId", "sampleLat", "sampleLon"):
        assert expected_col in df.columns


def test_navdat_citations_table_filtered_by_authors(backend):
    """
    Test that the citations table is populated and reflects the authors
    filter (confirmed live: authors=Walker returns Walker-authored records).
    """
    table_names = list(backend._cache.keys())
    if "citations" not in table_names:
        pytest.skip("The query returned no citation records")

    citations = backend.get_table("citations", dict_return=False)
    assert isinstance(citations, pd.DataFrame)
    assert not citations.empty
    assert "citationAuthors" in citations.columns
    # every returned citation's author string should mention 'walker'
    # (case-insensitive; API returns lowercase full names, e.g. "walker, d.")
    assert citations["citationAuthors"].str.lower().str.contains("walker").all()


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_navdat_query_artifacts_not_supported(backend):
    """Test that query_artifacts raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("SELECT * FROM samples")


def test_navdat_get_table(backend):
    """Test getting table data as DataFrame or OrderedDict."""
    df = backend.get_table("samples", dict_return=False)
    assert isinstance(df, pd.DataFrame)

    dict_data = backend.get_table("samples", dict_return=True)
    assert isinstance(dict_data, OrderedDict)

    with pytest.raises(ValueError):
        backend.get_table("nonexistent_table")


def test_navdat_get_schema(backend):
    """Test get_schema returns SQL-style CREATE TABLE format."""
    schema = backend.get_schema()
    assert isinstance(schema, str)
    assert "CREATE TABLE" in schema
    assert "samples" in schema

    samples_schema = backend.get_schema("samples")
    assert "CREATE TABLE samples" in samples_schema


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_navdat_find_methods(backend):
    """Test all find methods work correctly."""
    results = backend.find("walker")
    assert isinstance(results, list)

    tables = backend.find_table("samples")
    assert isinstance(tables, list)
    assert any("samples" in table.t_name for table in tables)

    columns = backend.find_column("sample")
    assert isinstance(columns, list)

    cells = backend.find_cell("walker")
    assert isinstance(cells, list)
    assert all(cell.type == "row" for cell in cells)

def test_navdat_find_column_with_range(backend):
    """Test find_column range support for numeric columns."""
    matches = backend.find_column("sampleLat", range=True)

    assert isinstance(matches, list)

    if not matches:
        pytest.skip("No sampleLat column found in loaded NAVDAT results")

    match = next(item for item in matches if "sampleLat" in item.c_name)

    assert match.type == "column"
    assert isinstance(match.value, dict)
    assert "min" in match.value
    assert "max" in match.value

def test_navdat_find_cell_cell_type_support(backend):
    """Test find_cell(row=False) returns individual cell matches."""
    cells = backend.find_cell("walker", row=False)

    assert isinstance(cells, list)

    if not cells:
        pytest.skip("Loaded NAVDAT results did not contain a walker cell match")

    assert all(cell.type == "cell" for cell in cells)
    assert all(len(cell.c_name) == 1 for cell in cells)

def test_navdat_find_relation_publication_year(backend):
    """Test find_relation against a known numeric column, if citations exist."""
    if "citations" not in backend._cache:
        pytest.skip("No citations table to test find_relation against")

    result = backend.find_relation("citationPublicationYear", "> 1900")
    assert isinstance(result, list)


# =============================================================================
# 5) Display & Summary
# =============================================================================

def test_navdat_list(backend):
    """Test list method returns table names."""
    table_names = backend.list(collection=True)
    assert isinstance(table_names, list)
    assert "samples" in table_names


def test_navdat_summary(backend):
    """Test summary returns proper format with SQL-style types."""
    summary_list = backend.summary()
    assert isinstance(summary_list, list)
    assert len(summary_list) >= 2

    for dataframe in summary_list[1:]:
        assert isinstance(dataframe, pd.DataFrame)
        assert "column" in dataframe.columns
        assert "type" in dataframe.columns

    summary_single = backend.summary("samples")
    assert isinstance(summary_single, pd.DataFrame)


def test_navdat_display(backend):
    """Test display method returns a DataFrame."""
    result = backend.display("samples", num_rows=5)

    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 5

def test_navdat_display_cols(backend):
    """Test display with selected columns."""
    result = backend.display(
        "samples",
        num_rows=5,
        display_cols=["sampleName", "sampleId"],
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["sampleName", "sampleId"]


def test_navdat_display_requires_table_name(backend):
    """Test display requires table_name."""
    with pytest.raises(TypeError):
        backend.display()


# =============================================================================
# 6) Filtering and Multiple Queries
# =============================================================================

def test_navdat_filters(filtered_backend):
    """Test loading data from the configured filtered queries."""
    assert filtered_backend._loaded is True
    assert "samples" in filtered_backend._cache


def test_navdat_multiple_queries_deduplicate(filtered_backend):
    """Test that identical rows returned by overlapping queries are deduplicated."""
    samples = filtered_backend._cache.get("samples", {})
    if not samples:
        pytest.skip("The queries returned no sample rows")

    df = pd.DataFrame(samples)
    assert len(df) == len(df.drop_duplicates())


# =============================================================================
# 7) Read-Only Enforcement
# =============================================================================

def test_navdat_read_only(backend):
    """Test that write operations raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})


# =============================================================================
# 8) Lifecycle
# =============================================================================

def test_navdat_close():
    """
    Test that close() properly resets backend state.

    This test must use its own instance because close() destructively
    mutates the backend and would invalidate a shared fixture.
    """
    backend = NAVDAT(
        params={
            "sampleNames": "(2.151) 12.40-12.85",
            "size": 1,
        }
    )
    assert backend._loaded is True
    assert len(backend._cache) > 0

    backend.close()

    assert backend._loaded is False
    assert len(backend._cache) == 0


# =============================================================================
# 9) Edge Cases
# =============================================================================

def test_navdat_empty_results(empty_backend):
    """Test handling of queries that return no results (confirmed live: 0 rows)."""
    samples = empty_backend._cache.get("samples", {})
    if samples:
        first_col = next(iter(samples.values()))
        assert len(first_col) == 0