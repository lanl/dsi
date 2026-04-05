# examples/ndp/7.notebook.py
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Fetch artifacts
backend.query_artifacts(
    query=None,
    kwargs={"keywords": "wildfire", "limit": 5}
)

# Notebook-style preview
backend.notebook()