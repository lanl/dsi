# dsi/backends/tests/test_ndp.py
from collections import OrderedDict
from dsi.core import Terminal
import pytest
import pandas as pd

# ----------------------------
# 1) Test loading NDP backend with specific parameters
# ----------------------------
def test_ndp_load_and_query():
    """Test NDP backend loads with specific climate data parameters."""
    terminal = Terminal()
    
    # Load NDP backend with climate search
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate temperature precipitation",
            "organization": "noaa-gov",
            "limit": 10
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Verify data was loaded
    assert backend._loaded is True
    assert len(backend._cache) > 0
    assert "datasets" in backend._cache
    
    terminal.close()


# ----------------------------
# 2) Test validate_connection
# ----------------------------
def test_ndp_validate_connection():
    """Test connection validation to CKAN API."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Should not raise an exception
    assert backend.validate_connection() is True
    
    terminal.close()


# ----------------------------
# 3) Test process_artifacts structure
# ----------------------------
def test_ndp_process_structure():
    """Test that processed artifacts have correct tiered structure."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "ocean temperature",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    artifacts = backend.process_artifacts()
    
    # Verify tiered structure
    assert isinstance(artifacts, OrderedDict)
    assert "datasets" in artifacts
    assert isinstance(artifacts["datasets"], OrderedDict)
    
    # Check dataset table has expected columns
    dataset_cols = list(artifacts["datasets"].keys())
    assert "id" in dataset_cols
    assert "title" in dataset_cols
    assert "num_resources" in dataset_cols
    
    terminal.close()


# ----------------------------
# 4) Test table name resolution (dataset_id vs dataset_title)
# ----------------------------
def test_ndp_table_name_resolution():
    """Test that both dataset_id and dataset_title can be used to access tables."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate data",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Should have some resource tables
    if backend._resource_tables:
        # Get first resource table
        dataset_title = backend._resource_tables[0]
        dataset_id = backend._dataset_title_map.get(dataset_title)
        
        if dataset_id:
            # Both should resolve to same table
            resolved_by_title = backend._resolve_table_name(dataset_title)
            resolved_by_id = backend._resolve_table_name(dataset_id)
            assert resolved_by_title == resolved_by_id == dataset_title
    
    terminal.close()


# ----------------------------
# 5) Test query_artifacts with pandas query
# ----------------------------
def test_ndp_query_with_pandas():
    """Test querying loaded data with pandas query string."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate data",
            "limit": 10
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Query datasets with resources
    result = backend.query_artifacts("num_resources > 0", dict_return=True)
    
    assert isinstance(result, dict)
    # Should have results if any datasets have resources
    if result:
        assert any(key in result for key in backend._cache.keys())
    
    terminal.close()


# ----------------------------
# 6) Test invalid pandas query raises error
# ----------------------------
def test_ndp_query_invalid():
    """Test that invalid pandas queries raise appropriate errors."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "temperature",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    with pytest.raises(ValueError):
        backend.query_artifacts("INVALID SYNTAX ###")
    
    terminal.close()


# ----------------------------
# 7) Test find methods
# ----------------------------
def test_ndp_find_operations():
    """Test find operations across tables, columns, and cells."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate temperature",
            "limit": 8
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Test finding tables
    tables_found = backend.find_table("datasets")
    assert len(tables_found) > 0
    assert any("datasets" in t.t_name for t in tables_found)
    
    # Test finding columns
    columns_found = backend.find_column("title")
    assert len(columns_found) > 0
    assert any("title" in c.c_name for c in columns_found)
    
    # Test finding cells
    cells_found = backend.find_cell("climate")
    if cells_found:
        assert all(hasattr(cell, 'type') and cell.type == "cell" for cell in cells_found)
    
    terminal.close()


# ----------------------------
# 8) Test combined find()
# ----------------------------
def test_ndp_find_combined():
    """Test the general find() method that searches everywhere."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "ocean data",
            "limit": 7
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    results = backend.find("title")
    assert isinstance(results, list)
    
    terminal.close()


# ----------------------------
# 9) Test find_relation (should return empty list)
# ----------------------------
def test_ndp_find_relation():
    """Test that find_relation returns empty list (not supported)."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    result = backend.find_relation("column_name", "= 'value'")
    assert isinstance(result, list)
    assert len(result) == 0
    
    terminal.close()


# ----------------------------
# 10) Test URL validation on resource tables
# ----------------------------
def test_ndp_validate_urls():
    """Test URL validation for dataset resources."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate precipitation",
            "organization": "noaa-gov",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    backend.validate_urls()
    
    # Check that url_valid was added to resource tables
    for table_name in backend._resource_tables:
        if table_name in backend._cache:
            table = backend._cache[table_name]
            if "url" in table and len(table["url"]) > 0:
                assert "url_valid" in table
                assert all(isinstance(v, bool) for v in table["url_valid"])
                assert len(table["url_valid"]) == len(table["url"])
    
    terminal.close()


# ----------------------------
# 11) Test list method
# ----------------------------
def test_ndp_list():
    """Test list method returns table names."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "atmospheric pressure",
            "limit": 6
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Test list with collection=True
    table_names = backend.list(collection=True)
    assert isinstance(table_names, (dict_keys, list))
    assert "datasets" in table_names
    
    terminal.close()


# ----------------------------
# 12) Test summary method
# ----------------------------
def test_ndp_summary():
    """Test summary returns DataFrame with table metadata."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Test summary for all tables
    summary_df = backend.summary()
    assert isinstance(summary_df, pd.DataFrame)
    assert not summary_df.empty
    assert "table_name" in summary_df.columns
    assert "num_rows" in summary_df.columns
    
    # Test summary for specific table
    summary_single = backend.summary("datasets")
    assert isinstance(summary_single, pd.DataFrame)
    
    terminal.close()


# ----------------------------
# 13) Test get_table method
# ----------------------------
def test_ndp_get_table():
    """Test getting table data as DataFrame or OrderedDict."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Get as DataFrame
    df = backend.get_table("datasets", dict_return=False)
    assert isinstance(df, pd.DataFrame)
    
    # Get as OrderedDict
    dict_data = backend.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)
    
    terminal.close()


# ----------------------------
# 14) Test get_schema method
# ----------------------------
def test_ndp_get_schema():
    """Test that get_schema returns informative message."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    schema = backend.get_schema()
    assert isinstance(schema, str)
    assert "NDP" in schema
    assert "read-only" in schema
    
    terminal.close()


# ----------------------------
# 15) Test read-only enforcement
# ----------------------------
def test_ndp_read_only():
    """Test that NDP backend is properly read-only."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "data",
            "limit": 3
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    with pytest.raises(NotImplementedError):
        backend.ingest_artifacts({})
    
    terminal.close()


# ----------------------------
# 16) Test display method
# ----------------------------
def test_ndp_display():
    """Test display method for tables."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate ocean",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    # Display datasets table
    result = backend.display("datasets", num_rows=10)
    assert isinstance(result, pd.DataFrame)
    assert len(result) <= 10
    
    terminal.close()


# ----------------------------
# 17) Test with different organizations
# ----------------------------
def test_ndp_organization_filtering():
    """Test querying with organization filter."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "organization": "noaa-gov",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    data = backend.process_artifacts()
    
    assert "datasets" in data
    assert len(data["datasets"]) > 0
    
    terminal.close()


# ----------------------------
# 18) Test with tags filter
# ----------------------------
def test_ndp_tags_filtering():
    """Test querying with tags filter."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "temperature",
            "tags": ["climate", "weather"],
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    assert backend._loaded is True
    data = backend.process_artifacts()
    assert "datasets" in data
    
    terminal.close()


# ----------------------------
# 19) Test with format filter
# ----------------------------
def test_ndp_format_filtering():
    """Test querying with resource format filter."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "data",
            "formats": ["CSV", "JSON"],
            "limit": 10
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    assert backend._loaded is True
    
    terminal.close()


# ----------------------------
# 20) Test close method
# ----------------------------
def test_ndp_close():
    """Test that close() properly resets backend state."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    assert backend._loaded is True
    assert len(backend._cache) > 0
    
    backend.close()
    
    assert backend._loaded is False
    assert len(backend._cache) == 0
    assert len(backend._resource_tables) == 0
    
    terminal.close()


# ----------------------------
# 21) Test overwrite_table raises NotImplementedError
# ----------------------------
def test_ndp_overwrite_table():
    """Test that overwrite_table raises NotImplementedError."""
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "limit": 5
        }
    )
    
    backend = terminal.active_modules["back-read"][0]
    
    with pytest.raises(NotImplementedError):
        backend.overwrite_table("datasets", pd.DataFrame())
    
    terminal.close()