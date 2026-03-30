# examples/ckan/2.process.py
from dsi.core import Terminal

def test_process_artifacts():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": "climate", "limit": 5})
    
    processed = backend.process_artifacts(kwargs={})
    assert processed == backend._cache, "Processed artifacts should match _cache"

    print("2.process.py passed")

if __name__ == "__main__":
    test_process_artifacts()