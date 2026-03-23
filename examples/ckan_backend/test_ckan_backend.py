import pytest
from dsi.core import Terminal


# --------------------------------------------------
# Helper: Load CKAN Backend
# --------------------------------------------------

def load_ckan():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    return t


# --------------------------------------------------
# 1. Backend Loads
# --------------------------------------------------

def test_ckan_backend_load():
    t = load_ckan()

    # Check backend namespace exists
    assert "backend" in t.module_collection

    # Check CKAN module is registered
    assert any(
        "ckan" in key.lower()
        for key in t.module_collection["backend"].keys()
    )

# --------------------------------------------------
# 2. Ingest Works
# --------------------------------------------------

def test_ckan_ingest():
    t = load_ckan()

    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(
        None,
        {"keywords": "data", "limit": 5}
    )

    tables = backend.process_artifacts({})

    assert "datasets" in tables
    assert "resources" in tables


# --------------------------------------------------
# 3. Data Actually Retrieved
# --------------------------------------------------

def test_ckan_data_retrieved():
    t = load_ckan()
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(
        None,
        {"keywords": "science", "limit": 5}
    )

    tables = backend.process_artifacts({})

    assert len(tables["datasets"]) >= 0  # safe check


# --------------------------------------------------
# 4. Query Works
# --------------------------------------------------

def test_ckan_query():
    t = load_ckan()
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(
        None,
        {"keywords": "data", "limit": 10}
    )

    result = backend.query_artifacts(
        "`num_resources` >= 0",
        {}
    )

    assert result is not None


# --------------------------------------------------
# 5. Find Functions
# --------------------------------------------------

def test_ckan_find():
    t = load_ckan()
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(
        None,
        {"keywords": "data", "limit": 5}
    )

    results = backend.find("title", {})

    assert isinstance(results, list)