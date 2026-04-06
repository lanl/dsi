# tests/test_ndp.py
from collections import OrderedDict
from dsi.backends.ndp import NDP

# ----------------------------
# 1) Test querying artifacts
# ----------------------------
def test_ndp_query_artifacts():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "climate", "limit": 5})
    artifacts = backend.process_artifacts()
    assert "datasets" in artifacts
    assert "resources" in artifacts
    backend.close()

# ----------------------------
# 2) Test process_artifacts structure
# ----------------------------
def test_ndp_process_structure():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "energy", "limit": 5})
    artifacts = backend.process_artifacts()
    assert isinstance(artifacts["datasets"], dict)
    assert isinstance(artifacts["resources"], dict)
    backend.close()

# ----------------------------
# 3) Test query_in_memory
# ----------------------------
def test_ndp_query_in_memory():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "science", "limit": 5})
    result = backend.query_in_memory("`num_resources` >= 0", {"table": "datasets"})
    assert isinstance(result, OrderedDict)
    assert all(len(v) > 0 for v in result.values())
    backend.close()

# ----------------------------
# 4) Test invalid query_in_memory raises
# ----------------------------
def test_ndp_query_invalid():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "science", "limit": 5})
    try:
        backend.query_in_memory("INVALID QUERY ###")
    except ValueError:
        pass
    else:
        assert False, "Invalid query did not raise ValueError"
    backend.close()

# ----------------------------
# 5) Test find_table / find_column / find_cell
# ----------------------------
def test_ndp_find_table_column_cell():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "science", "limit": 5})

    tables_found = backend.find_table("datasets")
    assert any("datasets" in t.t_name for t in tables_found)

    columns_found = backend.find_column("title")
    assert any("title" in c.c_name for c in columns_found)

    cells_found = backend.find_cell("Canada")
    if cells_found:
        for cell in cells_found:
            assert cell.type == "cell"

    backend.close()

# ----------------------------
# 6) Test combined find()
# ----------------------------
def test_ndp_find_combined():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "science", "limit": 5})
    results = backend.find("title")
    assert isinstance(results, list)
    backend.close()

# ----------------------------
# 7) Test URL validation
# ----------------------------
def test_ndp_validate_urls():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "transport", "limit": 5})
    backend.validate_urls()
    resources = backend.process_artifacts()["resources"]
    assert "url_valid" in resources
    assert all(isinstance(v, bool) for v in resources["url_valid"])
    assert len(resources["url_valid"]) == len(resources["url"])
    backend.close()

# ----------------------------
# 8) Test notebook / inspect_artifacts
# ----------------------------
def test_ndp_notebook_and_inspect():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "environment", "limit": 5})
    backend.notebook()  # ensure no exceptions
    meta = backend.inspect_artifacts()
    assert meta["loaded"] is True
    assert "datasets" in meta["tables"]
    assert "resources" in meta["tables"]
    backend.close()

# ----------------------------
# 9) Test read-only enforcement
# ----------------------------
def test_ndp_read_only():
    backend = NDP()
    try:
        backend.put_artifacts({}, {})
    except NotImplementedError:
        pass
    else:
        assert False, "put_artifacts did not raise NotImplementedError"
    backend.close()

# ----------------------------
# 10) Test metadata helpers
# ----------------------------
def test_ndp_metadata_methods():
    backend = NDP()
    backend.query_artifacts(None, {"keywords": "earth", "limit": 3})
    assert backend.git_commit_sha() == "ndp-ckan-readonly-backend"
    assert isinstance(backend.get_artifacts(), dict)
    assert isinstance(backend.read_to_artifacts(), dict)
    backend.close()