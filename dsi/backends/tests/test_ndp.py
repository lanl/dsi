"""
NDP Backend Function Tests

Essential tests for NDP backend core functionality.
"""

from collections import OrderedDict
import pytest
import pandas as pd
from dsi.backends.ndp import NDP


# =============================================================================
# 1) Initialization & Connection
# =============================================================================

def test_ndp_initialization():
    """Test NDP backend initializes correctly."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "datasets" in backend._cache
    
    backend.close()


def test_ndp_validate_connection():
    """Test connection validation to CKAN API."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    assert backend.validate_connection() is True
    backend.close()


# =============================================================================
# 2) Data Loading & Structure
# =============================================================================

def test_ndp_unified_resources_table():
    """Test that resources from all datasets are in one unified table."""
    backend = NDP(params={"keywords": "climate data", "limit": 5})
    
    table_names = list(backend._cache.keys())
    assert "datasets" in table_names
    
    # Verify unified resources structure
    if "resources" in table_names:
        resources = backend._cache["resources"]
        assert "dataset_id" in resources
        assert "resource_id" in resources
        assert "url" in resources
        
        # Verify foreign key relationship
        datasets = backend._cache["datasets"]
        dataset_ids = set(datasets["id"])
        resource_dataset_ids = set(resources["dataset_id"])
        assert resource_dataset_ids.issubset(dataset_ids)
    
    backend.close()


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_ndp_query_artifacts_not_supported():
    """Test that query_artifacts raises NotImplementedError."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("SELECT * FROM datasets")
    
    backend.close()


def test_ndp_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    # Get as DataFrame
    df = backend.get_table("datasets", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Get as OrderedDict
    dict_data = backend.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    
    # Test invalid table
    with pytest.raises(ValueError):
        backend.get_table("nonexistent_table")
    
    backend.close()


def test_ndp_get_schema():
    """Test get_schema returns SQL-style CREATE TABLE format."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    # Get all schemas
    schema = backend.get_schema()
    assert isinstance(schema, str)
    assert "CREATE TABLE" in schema
    assert "datasets" in schema
    
    # Get single table schema
    datasets_schema = backend.get_schema("datasets")
    assert "CREATE TABLE datasets" in datasets_schema
    
    backend.close()


# =============================================================================
# 4) Find Operations
# =============================================================================

def test_ndp_find_methods():
    """Test all find methods work correctly."""
    backend = NDP(params={"keywords": "climate temperature", "limit": 8})
    
    # Test find (all levels)
    results = backend.find("title")
    assert isinstance(results, list)
    
    # Test find_table
    tables = backend.find_table("datasets")
    assert isinstance(tables, list)
    assert any("datasets" in t.t_name for t in tables)
    
    # Test find_column
    columns = backend.find_column("title")
    assert isinstance(columns, list)
    assert any("title" in c.c_name for c in columns)
    
    # Test find_cell
    cells = backend.find_cell("climate")
    assert isinstance(cells, list)
    
    backend.close()


def test_ndp_find_relation():
    """Test find_relation with various operators."""
    backend = NDP(params={"keywords": "climate", "limit": 10})
    
    # Test numeric comparison
    result = backend.find_relation("num_resources", "> 2")
    assert isinstance(result, list)
    
    # Test contains
    result = backend.find_relation("title", "~~ climate")
    assert isinstance(result, list)
    
    # Test other operators
    for operator in [">= 3", "<= 8", "== 5", "!= 0"]:
        result = backend.find_relation("num_resources", operator)
        assert isinstance(result, list)
    
    backend.close()


# =============================================================================
# 5) URL Validation
# =============================================================================

def test_ndp_validate_urls():
    """Test URL validation for resources table."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    if "resources" not in backend._cache:
        backend.close()
        return
    
    backend.validate_urls()
    
    resources_table = backend._cache.get("resources", {})
    if "url" in resources_table and len(resources_table["url"]) > 0:
        assert "url_valid" in resources_table
        assert all(isinstance(v, bool) for v in resources_table["url_valid"])
    
    backend.close()


# =============================================================================
# 6) Display & Summary
# =============================================================================

def test_ndp_list():
    """Test list method returns table names."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    table_names = backend.list(collection=True)
    assert "datasets" in table_names
    assert isinstance(table_names, list)
    
    backend.close()


def test_ndp_summary():
    """Test summary returns proper format with SQL-style types."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    # Test summary for all tables
    summary_list = backend.summary()
    assert isinstance(summary_list, list)
    assert len(summary_list) >= 2
    
    # Check SQL-style types
    for df in summary_list[1:]:
        assert isinstance(df, pd.DataFrame)
        assert "column" in df.columns
        assert "type" in df.columns
    
    # Test single table summary
    summary_single = backend.summary("datasets")
    assert isinstance(summary_single, pd.DataFrame)
    
    backend.close()


def test_ndp_display():
    """Test display method for tables."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    # Should not raise exception
    backend.display("datasets", num_rows=5)
    
    # Test with specific columns
    backend.display("datasets", num_rows=5, display_cols=["title", "organization"])
    
    backend.close()


# =============================================================================
# 7) Filtering
# =============================================================================

def test_ndp_filters():
    """Test loading data with various filters."""
    # Organization filter
    backend1 = NDP(params={
        "keywords": "climate",
        "organization": "California Landscape Metrics",
        "limit": 5
    })
    assert backend1._loaded is True
    backend1.close()
    
    # Tags filter
    backend2 = NDP(params={
        "keywords": "temperature",
        "tags": ["climate", "weather"],
        "limit": 5
    })
    assert backend2._loaded is True
    backend2.close()
    
    # Groups filter
    backend3 = NDP(params={
        "keywords": "data",
        "groups": ["data_hub_cc_wstc"],
        "limit": 5
    })
    assert backend3._loaded is True
    backend3.close()
    
    # Format filter
    backend4 = NDP(params={
        "keywords": "data",
        "formats": ["CSV", "JSON"],
        "limit": 5
    })
    assert backend4._loaded is True
    backend4.close()


def test_ndp_multiple_queries():
    """Test loading data with multiple queries and deduplication."""
    backend = NDP(params=[
        {"keywords": "climate", "limit": 5},
        {"organization": "USGS", "limit": 5},
        {"tags": ["temperature"], "limit": 5}
    ])
    
    assert backend._loaded is True
    
    # Verify deduplication
    datasets = backend._cache.get("datasets", {})
    if "id" in datasets:
        dataset_ids = datasets["id"]
        assert len(dataset_ids) == len(set(dataset_ids))
    
    backend.close()


# =============================================================================
# 8) Read-Only Enforcement
# =============================================================================

def test_ndp_read_only():
    """Test that write operations raise NotImplementedError."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})
    
    with pytest.raises(NotImplementedError):
        backend.overwrite_table("datasets", pd.DataFrame())
    
    backend.close()


# =============================================================================
# 9) Lifecycle
# =============================================================================

def test_ndp_close():
    """Test that close() properly resets backend state."""
    backend = NDP(params={"keywords": "climate", "limit": 5})
    
    # Before close
    assert backend._loaded is True
    assert len(backend._cache) > 0
    
    # Close
    backend.close()
    
    # After close
    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._dataset_id_map) == 0


# =============================================================================
# 10) Edge Cases
# =============================================================================

def test_ndp_empty_results():
    """Test handling of queries that return no results."""
    backend = NDP(params={
        "keywords": "zzzzznonexistentkeywordzzzzz",
        "limit": 10
    })
    
    datasets = backend._cache.get("datasets", {})
    if datasets and "id" in datasets:
        assert len(datasets["id"]) == 0
    
    backend.close()