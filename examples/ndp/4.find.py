# examples/ndp/4.find.py
from dsi.core import Terminal

verbose = True

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Fetch artifacts
backend.query_artifacts(
    query=None,
    kwargs={"keywords": "climate", "limit": 5}
)

# Find tables, columns, and cells
tables_found = backend.find_table("datasets")
columns_found = backend.find_column("title")
cells_found = backend.find_cell("Canada")

if verbose:
    print("Tables matching 'datasets':", [v.t_name for v in tables_found])
    print("Columns matching 'title':", [v.c_name for v in columns_found])
    print("Cells matching 'Canada':", [(v.t_name, v.c_name, v.value) for v in cells_found])