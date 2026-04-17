"""
NDP Backend Function Tests

Tests NDP backend methods directly without Terminal integration.
Mirrors structure of test_sqlite.py
"""

from collections import OrderedDict
import pytest
import pandas as pd
from dsi.backends.ndp import NDP


# =============================================================================
# 1) Basic Backend Initialization
# =============================================================================

def test_ndp_initialization():
    """Test NDP backend initializes correctly."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "datasets" in backend._cache
    
    backend.close()


def test_ndp_validate_connection():
    """Test connection validation to CKAN API."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Should return True without raising exception
    assert backend.validate_connection() is True
    
    backend.close()


def test_ndp_invalid_url():
    """Test that invalid URL raises appropriate error."""
    with pytest.raises(ValueError):
        NDP(url="not-a-valid-url")


# =============================================================================
# 2) Data Loading and Structure
# =============================================================================

def test_ndp_load_initial_data():
    """Test that initial data load creates proper structure."""
    backend = NDP(
        params={
            "keywords": "ocean temperature",
            "limit": 5
        }
    )
    
    # Verify structure
    assert isinstance(backend._cache, OrderedDict)
    assert "datasets" in backend._cache
    assert isinstance(backend._cache["datasets"], OrderedDict)
    
    # Check dataset table has expected columns
    dataset_cols = list(backend._cache["datasets"].keys())
    assert "id" in dataset_cols
    assert "title" in dataset_cols
    assert "num_resources" in dataset_cols
    
    backend.close()


def test_ndp_table_name_resolution():
    """Test that both dataset_id and dataset_title resolve correctly."""
    backend = NDP(
        params={"keywords": "climate data", "limit": 5}
    )
    
    # If we have resource tables, test resolution
    if backend._resource_tables:
        dataset_title = backend._resource_tables[0]
        dataset_id = backend._dataset_title_map.get(dataset_title)
        
        if dataset_id:
            # Both should resolve to same table
            resolved_by_title = backend._resolve_table_name(dataset_title)
            resolved_by_id = backend._resolve_table_name(dataset_id)
            assert resolved_by_title == resolved_by_id == dataset_title
    
    backend.close()


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_ndp_query_artifacts():
    """Test querying loaded data with pandas query string."""
    backend = NDP(
        params={"keywords": "climate data", "limit": 10}
    )
    
    # Query datasets with resources
    result = backend.query_artifacts("num_resources > 0", dict_return=True)
    
    # Assertions
    assert isinstance(result, dict)
    
    # If we got results, verify structure
    if result:
        for table_name, table_data in result.items():
            assert isinstance(table_data, dict)
            # Each column should be a list
            for col_values in table_data.values():
                assert isinstance(col_values, list)
    
    backend.close()


def test_ndp_query_invalid():
    """Test that invalid pandas queries raise appropriate errors."""
    backend = NDP(
        params={"keywords": "temperature", "limit": 5}
    )
    
    with pytest.raises(ValueError):
        backend.query_artifacts("INVALID SYNTAX ###")
    
    backend.close()


def test_ndp_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Get as DataFrame
    df = backend.get_table("datasets", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Get as OrderedDict
    dict_data = backend.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0
    
    backend.close()


def test_ndp_get_schema():
    """Test that get_schema returns informative message."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    schema = backend.get_schema()
    assert isinstance(schema, str)
    assert "NDP" in schema
    assert "read-only" in schema.lower()
    
    backend.close()


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_ndp_find():
    """Test general find operation across all levels."""
    backend = NDP(
        params={"keywords": "climate temperature", "limit": 8}
    )
    
    results = backend.find("title")
    assert isinstance(results, list)
    
    backend.close()


def test_ndp_find_table():
    """Test finding tables by name."""
    backend = NDP(
        params={"keywords": "climate temperature", "limit": 8}
    )
    
    tables_found = backend.find_table("datasets")
    assert isinstance(tables_found, list)
    assert len(tables_found) > 0
    assert any("datasets" in t.t_name for t in tables_found)
    
    backend.close()


def test_ndp_find_column():
    """Test finding columns by name."""
    backend = NDP(
        params={"keywords": "climate temperature", "limit": 8}
    )
    
    columns_found = backend.find_column("title")
    assert isinstance(columns_found, list)
    assert len(columns_found) > 0
    assert any("title" in c.c_name for c in columns_found)
    
    backend.close()


def test_ndp_find_cell():
    """Test finding cells by value."""
    backend = NDP(
        params={"keywords": "climate temperature", "limit": 8}
    )
    
    cells_found = backend.find_cell("climate")
    assert isinstance(cells_found, list)
    if cells_found:  # May be empty
        assert all(hasattr(cell, 'type') and cell.type == "cell" for cell in cells_found)
    
    backend.close()


def test_ndp_find_relation():
    """Test that find_relation returns empty list (not supported)."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    result = backend.find_relation("column_name", "= 'value'")
    assert isinstance(result, list)
    assert len(result) == 0  # NDP doesn't support relational queries
    
    backend.close()


# =============================================================================
# 5) URL Validation
# =============================================================================

def test_ndp_validate_urls():
    """Test URL validation for dataset resources."""
    backend = NDP(
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend.validate_urls()
    
    # Check that url_valid was added to resource tables
    for table_name in backend._resource_tables:
        if table_name in backend._cache:
            table = backend._cache[table_name]
            if "url" in table and len(table["url"]) > 0:
                # url_valid should exist and be boolean list
                assert "url_valid" in table
                assert all(isinstance(v, bool) for v in table["url_valid"])
                assert len(table["url_valid"]) == len(table["url"])
    
    backend.close()


# =============================================================================
# 6) List and Summary
# =============================================================================

def test_ndp_list():
    """Test list method returns table names."""
    backend = NDP(
        params={"keywords": "climate", "limit": 6}
    )
    
    # Test list with collection=True
    table_names = backend.list(collection=True)
    assert "datasets" in table_names
    assert isinstance(table_names, (list, dict, type({}.keys())))
    
    backend.close()


def test_ndp_num_tables():
    """Test num_tables prints correct count."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Should not raise exception
    backend.num_tables()
    
    backend.close()


def test_ndp_summary():
    """Test summary returns DataFrame with table metadata."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Test summary for all tables
    summary_df = backend.summary()
    assert isinstance(summary_df, pd.DataFrame)
    assert not summary_df.empty
    assert "table_name" in summary_df.columns
    assert "num_rows" in summary_df.columns
    
    # Test summary for specific table
    summary_single = backend.summary("datasets")
    assert isinstance(summary_single, pd.DataFrame)
    
    backend.close()


def test_ndp_display():
    """Test display method for tables."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Display datasets table
    result = backend.display("datasets", num_rows=10)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10
    
    backend.close()


# =============================================================================
# 7) Process Artifacts
# =============================================================================

def test_ndp_process_artifacts():
    """Test that processed artifacts have correct structure."""
    backend = NDP(
        params={"keywords": "ocean temperature", "limit": 5}
    )
    
    artifacts = backend.process_artifacts()
    
    # Verify structure
    assert isinstance(artifacts, OrderedDict)
    assert "datasets" in artifacts
    assert isinstance(artifacts["datasets"], OrderedDict)
    
    backend.close()


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_ndp_ingest_artifacts():
    """Test that ingest_artifacts raises NotImplementedError."""
    backend = NDP(
        params={"keywords": "data", "limit": 3}
    )
    
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})
    
    backend.close()


def test_ndp_overwrite_table():
    """Test that overwrite_table raises NotImplementedError."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    with pytest.raises(NotImplementedError):
        backend.overwrite_table("datasets", pd.DataFrame())
    
    backend.close()


# =============================================================================
# 9) Filtering Tests
# =============================================================================

def test_ndp_organization_filter():
    """Test loading data with organization filter."""
    backend = NDP(
        params={
            "keywords": "climate",
            "organization": "california-landscape-metrics",
            "limit": 10
        }
    )
    
    data = backend.process_artifacts()
    
    assert "datasets" in data
    assert isinstance(data["datasets"], OrderedDict)
    
    # Check we got data (number of rows)
    if data["datasets"]:
        # Verify organization column
        assert "organization" in data["datasets"]
        
        orgs = data["datasets"]["organization"]
        # Check at least one is from California Landscape Metrics
        assert any(
            org and "California Landscape Metrics" in org 
            for org in orgs
        )
    
    backend.close()


def test_ndp_tags_filter():
    """Test loading data with tags filter."""
    backend = NDP(
        params={
            "keywords": "temperature",
            "tags": ["climate", "weather"],
            "limit": 5
        }
    )
    
    assert backend._loaded is True
    data = backend.process_artifacts()
    assert "datasets" in data
    
    backend.close()


def test_ndp_format_filter():
    """Test loading data with format filter."""
    backend = NDP(
        params={
            "keywords": "data",
            "formats": ["CSV", "JSON"],
            "limit": 10
        }
    )
    
    assert backend._loaded is True
    data = backend.process_artifacts()
    assert isinstance(data, OrderedDict)
    
    backend.close()


# =============================================================================
# 10) Lifecycle
# =============================================================================

def test_ndp_close():
    """Test that close() properly resets backend state."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Before close
    assert backend._loaded is True
    assert len(backend._cache) > 0
    
    # Close
    backend.close()
    
    # After close
    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._resource_tables) == 0


def test_ndp_notebook():
    """Test that notebook() doesn't raise errors."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Should not raise exception (even though it's a no-op)
    backend.notebook()
    
    backend.close()


def test_ndp_get_table_names():
    """Test extracting table names from query strings."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Test with datasets table
    names = backend.get_table_names("SELECT * FROM datasets WHERE title LIKE '%climate%'")
    assert "datasets" in names
    
    backend.close()