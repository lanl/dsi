# examples/ckan/1.ingest.py
from dsi.core import Terminal

def test_ingest_artifacts():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": "climate", "limit": 5})

    # Validate that data is loaded
    assert backend._loaded is True, "Backend should be marked as loaded"
    assert len(backend._cache["datasets"]) > 0, "Datasets should not be empty"
    assert len(backend._cache["resources"]) > 0, "Resources should not be empty"

    print("1.ingest.py passed")

if __name__ == "__main__":
    test_ingest_artifacts()