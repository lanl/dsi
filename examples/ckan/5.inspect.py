# examples/ckan/5.inspect.py
from dsi.core import Terminal

def test_inspect_and_get():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": "climate", "limit": 5})

    summary = backend.inspect_artifacts()
    assert "loaded" in summary and "tables" in summary
    assert summary["loaded"] is True

    artifacts1 = backend.get_artifacts()
    artifacts2 = backend.read_to_artifacts()
    assert artifacts1 == artifacts2, "get_artifacts and read_to_artifacts should be consistent"

    print("5.inspect.py passed")

if __name__ == "__main__":
    test_inspect_and_get()