# dsi/backends/tests/test_ndp.py
from collections import OrderedDict
from dsi.core import Terminal
import pytest

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
# 2) Test process_artifacts structure
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
# 3) Test query_artifacts with pandas query
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
        assert "datasets" in result or any("resources_" in key for key in result.keys())
    
    terminal.close()


# ----------------------------
# 4) Test invalid pandas query raises error
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
# 5) Test find methods
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
# 6) Test combined find()
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
# 7) Test URL validation on resource tables
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
# 8) Test list and summary methods
# ----------------------------
def test_ndp_list_and_summary():
    """Test metadata retrieval methods."""
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
    table_info = backend.list(collection=True)
    assert isinstance(table_info, dict)
    assert "datasets" in table_info
    assert "resources" in table_info
    assert isinstance(table_info["resources"], list)
    
    # Test summary
    summary_df = backend.summary()
    assert not summary_df.empty
    assert "table_name" in summary_df.columns
    assert "num_rows" in summary_df.columns
    
    terminal.close()


# ----------------------------
# 9) Test read-only enforcement
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
# 10) Test display method
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
    assert result is not None
    assert len(result) <= 10
    
    terminal.close()


# ----------------------------
# 11) Test with different organizations
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
# 12) Test with tags filter
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
# 13) Test with format filter
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
# 14) Test close method
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