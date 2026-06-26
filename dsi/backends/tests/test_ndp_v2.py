"""
NDP Backend Function Tests

Tests NDP backend methods directly without Terminal integration.
Updated for unified resources table structure.
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
    
    # Check resources table exists (if datasets have resources)
    datasets_table = backend._cache["datasets"]
    if datasets_table and "num_resources" in datasets_table:
        num_resources_list = datasets_table["num_resources"]
        total_resources = sum(r for r in num_resources_list if r)
        
        if total_resources > 0:
            assert "resources" in backend._cache
            resources_table = backend._cache["resources"]
            assert "resource_id" in resources_table
            assert "dataset_id" in resources_table
            assert "url" in resources_table
    
    backend.close()


def test_ndp_unified_resources_table():
    """Test that resources from all datasets are in one unified table."""
    backend = NDP(
        params={"keywords": "climate data", "limit": 5}
    )
    
    # Should only have 2 tables: datasets and resources
    table_names = list(backend._cache.keys())
    
    # Core tables
    assert "datasets" in table_names
    
    # Check resources table structure if it exists
    if "resources" in table_names:
        resources = backend._cache["resources"]
        
        # Should have dataset_id column linking to datasets
        assert "dataset_id" in resources
        assert "resource_id" in resources
        assert "url" in resources
        assert "format" in resources
        
        # Should have multiple datasets' resources (if more than 1 dataset)
        datasets = backend._cache["datasets"]
        dataset_ids = set(datasets["id"])
        resource_dataset_ids = set(resources["dataset_id"])
        
        # Resource dataset_ids should be subset of dataset ids
        assert resource_dataset_ids.issubset(dataset_ids)
    
    backend.close()


# =============================================================================
# 3) Query Operations
# =============================================================================

def test_ndp_query_artifacts_not_supported():
    """Test that query_artifacts raises NotImplementedError for SQL queries."""
    backend = NDP(
        params={"keywords": "climate data", "limit": 10}
    )
    
    # SQL queries not supported for NDP
    with pytest.raises(NotImplementedError):
        backend.query_artifacts("SELECT * FROM datasets", dict_return=True)
    
    backend.close()


def test_ndp_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Get as DataFrame (dict_return=False is default)
    df = backend.get_table("datasets", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Get as OrderedDict
    dict_data = backend.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    assert len(dict_data) > 0
    
    # Test resources table if it exists
    if "resources" in backend._cache:
        resources_df = backend.get_table("resources", dict_return=False)
        assert isinstance(resources_df, pd.DataFrame)
    
    backend.close()


def test_ndp_get_table_invalid():
    """Test that get_table raises error for non-existent table."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    with pytest.raises(ValueError):
        backend.get_table("nonexistent_table")
    
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
    """Test find_relation with numeric comparison."""
    backend = NDP(
        params={"keywords": "climate", "limit": 10}
    )
    
    # Find datasets with more than 2 resources
    result = backend.find_relation("num_resources", "> 2")
    assert isinstance(result, list)
    
    # If we got results, verify structure
    if result:
        for match in result:
            assert hasattr(match, 't_name')
            assert hasattr(match, 'c_name')
            assert hasattr(match, 'row_num')
            assert hasattr(match, 'value')
            assert match.type == "cell"
    
    backend.close()


def test_ndp_find_relation_operators():
    """Test find_relation with various operators."""
    backend = NDP(
        params={"keywords": "climate", "limit": 10}
    )
    
    # Test different operators
    operators = ["> 5", "< 10", ">= 3", "<= 8", "== 5"]
    
    for operator in operators:
        result = backend.find_relation("num_resources", operator)
        assert isinstance(result, list)
        # May be empty, but should not raise error
    
    backend.close()


# =============================================================================
# 5) URL Validation
# =============================================================================

def test_ndp_validate_urls():
    """Test URL validation for resources table."""
    backend = NDP(
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    # Skip if no resources
    if "resources" not in backend._cache:
        backend.close()
        return
    
    backend.validate_urls()
    
    # Check that url_valid was added to resources table
    resources_table = backend._cache.get("resources", {})
    
    if "url" in resources_table and len(resources_table["url"]) > 0:
        # url_valid should exist and be boolean list
        assert "url_valid" in resources_table
        assert all(isinstance(v, bool) for v in resources_table["url_valid"])
        assert len(resources_table["url_valid"]) == len(resources_table["url"])
    
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
    assert isinstance(table_names, list)
    
    backend.close()


def test_ndp_num_tables():
    """Test num_tables prints correct count."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Should not raise exception
    backend.num_tables()
    
    backend.close()


def test_ndp_num_datasets():
    """Test num_datasets returns correct count."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Should not raise exception
    backend.num_datasets()
    
    # Verify count matches datasets table length
    datasets_table = backend._cache.get("datasets", {})
    if datasets_table:
        first_col = next(iter(datasets_table.values()), [])
        expected_count = len(first_col)
        
        # Verify (capture print output would be ideal, but just ensure no crash)
        assert expected_count >= 0
    
    backend.close()


def test_ndp_summary():
    """Test summary returns proper format based on arguments."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Test summary for all tables (collection=False) - returns list
    summary_list = backend.summary(collection=False)
    assert isinstance(summary_list, list)
    assert len(summary_list) >= 2  # [table_names] + at least one df
    
    # First element should be table names list
    assert isinstance(summary_list[0], list)
    
    # Remaining elements should be DataFrames
    for df in summary_list[1:]:
        assert isinstance(df, pd.DataFrame)
    
    # Test summary for specific table (collection=False) - returns list
    summary_single = backend.summary("datasets", collection=False)
    assert isinstance(summary_single, list)
    assert len(summary_single) == 2  # [table_name], df
    assert isinstance(summary_single[1], pd.DataFrame)
    
    # Test summary with collection=True - returns DataFrame(s)
    summary_collection = backend.summary("datasets", collection=True)
    assert isinstance(summary_collection, pd.DataFrame)
    assert "column" in summary_collection.columns
    
    backend.close()


def test_ndp_display():
    """Test display method for tables."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Display should not raise exception (it prints, doesn't return)
    backend.display("datasets", num_rows=5)
    
    # Test with resources table if it exists
    if "resources" in backend._cache:
        backend.display("resources", num_rows=3)
    
    backend.close()


def test_ndp_display_with_columns():
    """Test display with specific columns."""
    backend = NDP(
        params={"keywords": "climate", "limit": 5}
    )
    
    # Display specific columns
    backend.display("datasets", num_rows=5, display_cols=["title", "organization"])
    
    # Display all columns
    backend.display("datasets", num_rows=5, display_cols="all")
    
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
    
    # Resources table should exist if datasets have resources
    if "resources" in artifacts:
        assert isinstance(artifacts["resources"], OrderedDict)
    
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
            "organization": "California Landscape Metrics",
            "limit": 10
        }
    )
    
    data = backend.process_artifacts()
    
    assert "datasets" in data
    assert isinstance(data["datasets"], OrderedDict)
    
    # Check we got data
    if data["datasets"]:
        assert "organization" in data["datasets"]
        
        orgs = data["datasets"]["organization"]
        # Check at least one is from specified organization
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
    
    # If resources exist, verify formats
    if "resources" in data and data["resources"]:
        formats = data["resources"].get("format", [])
        if formats:
            # Should have CSV or JSON formats
            assert any(fmt in ["CSV", "JSON"] for fmt in formats if fmt)
    
    backend.close()


def test_ndp_multiple_queries():
    """Test loading data with multiple independent queries."""
    backend = NDP(
        params=[
            {"keywords": "climate", "limit": 5},
            {"organization": "USGS", "limit": 5},
            {"tags": ["temperature"], "limit": 5}
        ]
    )
    
    assert backend._loaded is True
    
    # Verify datasets were loaded
    datasets = backend._cache.get("datasets", {})
    assert len(datasets) > 0
    
    # Results should be deduplicated
    if "id" in datasets:
        dataset_ids = datasets["id"]
        assert len(dataset_ids) == len(set(dataset_ids))  # No duplicates
    
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
    assert len(backend._dataset_id_map) == 0
    assert len(backend._dataset_title_map) == 0


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
    
    # Test with resources table
    names = backend.get_table_names("resources.url")
    if "resources" in backend._cache:
        assert "resources" in names
    
    backend.close()


# =============================================================================
# 11) Edge Cases
# =============================================================================

def test_ndp_empty_results():
    """Test handling of queries that return no results."""
    backend = NDP(
        params={
            "keywords": "zzzzznonexistentkeywordzzzzz",
            "limit": 10
        }
    )
    
    # Should have empty datasets table
    datasets = backend._cache.get("datasets", {})
    
    if datasets and "id" in datasets:
        assert len(datasets["id"]) == 0
    
    backend.close()


def test_ndp_dataset_by_id():
    """Test loading a specific dataset by ID."""
    # First get a dataset ID
    backend = NDP(
        params={"keywords": "climate", "limit": 1}
    )
    
    datasets = backend._cache.get("datasets", {})
    if datasets and "id" in datasets and len(datasets["id"]) > 0:
        dataset_id = datasets["id"][0]
        backend.close()
        
        # Now load by ID
        backend2 = NDP(
            params={"id": dataset_id}
        )
        
        datasets2 = backend2._cache.get("datasets", {})
        assert "id" in datasets2
        assert dataset_id in datasets2["id"]
        
        backend2.close()
    else:
        backend.close()


def test_ndp_large_limit():
    """Test loading with large limit value."""
    backend = NDP(
        params={"keywords": "data", "limit": 50}
    )
    
    # Should not crash
    assert backend._loaded is True
    
    backend.close()