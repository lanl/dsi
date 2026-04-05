import os
from collections import OrderedDict
from dsi.backends.webserver import Webserver
from DSI.dsi.backends.ndp import CKAN  # adjust import to your CKAN class

def test_ckan_ingest():
    backend = CKAN()
    backend.ingest_artifacts(kwargs={"keywords": "climate change", "limit": 5})
    artifacts = backend.process_artifacts()
    assert "datasets" in artifacts
    assert "resources" in artifacts
    backend.close()

def test_ckan_query():
    backend = CKAN()
    backend.ingest_artifacts(kwargs={"keywords": "energy", "limit": 10})
    result = backend.query_artifacts("`num_resources` > 0", {"table": "datasets"})
    assert isinstance(result, OrderedDict)
    assert all(len(v) > 0 for v in result.values())  # make sure columns are populated
    backend.close()

def test_ckan_find_table_column_cell():
    backend = CKAN()
    backend.ingest_artifacts(kwargs={"keywords": "science", "limit": 5})
    
    tables_found = backend.find_table("datasets")
    assert tables_found[0].t_name == "datasets"
    
    columns_found = backend.find_column("title")
    assert any("title" in col.c_name for col in columns_found)
    
    cells_found = backend.find_cell("Canada")  # depends on CKAN content
    if cells_found:
        for cell in cells_found:
            assert cell.type == "cell"
    
    backend.close()

def test_ckan_validate_urls():
    backend = CKAN()
    backend.ingest_artifacts(kwargs={"keywords": "transport", "limit": 5})
    backend.validate_urls()
    resources = backend.process_artifacts()["resources"]
    assert "url_valid" in resources
    assert all(isinstance(v, bool) for v in resources["url_valid"])
    backend.close()

def test_ckan_notebook_and_inspect():
    backend = CKAN()
    backend.ingest_artifacts(kwargs={"keywords": "environment", "limit": 5})
    backend.notebook()  # just ensure no exceptions
    meta = backend.inspect_artifacts()
    assert meta["loaded"] is True
    assert "datasets" in meta["tables"]
    assert "resources" in meta["tables"]
    backend.close()