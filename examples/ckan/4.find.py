# examples/ckan/4.find.py
from dsi.core import Terminal

def test_find_methods():
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": "climate", "limit": 5})

    # find_table
    tables = backend.find_table("datasets")
    assert isinstance(tables, list) or tables is None

    # find_column
    cols = backend.find_column("title")
    assert isinstance(cols, list) or cols is None

    # find_cell
    cells = backend.find_cell("climate")
    assert isinstance(cells, list) or cells is None

    # find
    all_matches = backend.find("climate")
    assert isinstance(all_matches, list)

    print("4.find.py passed")

if __name__ == "__main__":
    test_find_methods()