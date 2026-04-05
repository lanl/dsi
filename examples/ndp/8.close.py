# examples/ndp/8.close.py
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Fetch artifacts
backend.query_artifacts(
    query=None,
    kwargs={"keywords": "climate", "limit": 5}
)

# Close/reset backend
backend.close()

# Inspect backend state after closing
print("Backend state after close:", backend.inspect_artifacts())