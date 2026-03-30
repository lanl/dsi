# examples/ckan/3.query.py
from dsi.core import Terminal

def test_query_artifacts():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": "climate", "limit": 5})

    # Query datasets with num_resources > 0
    result = backend.query_artifacts("`num_resources` > 0", kwargs={"dict_return": True})
    assert isinstance(result, dict), "Query result should be a dict when dict_return=True"
    assert "id" in result, "Query result should contain 'id' column"

    print("3.query.py passed")

if __name__ == "__main__":
    test_query_artifacts()