# examples/ndp/5.inspect.py
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

# Inspect metadata and artifacts
meta = backend.inspect_artifacts()
artifacts_1 = backend.get_artifacts()
artifacts_2 = backend.read_to_artifacts()

if verbose:
    print("Inspect metadata summary:", meta)
    print("Artifacts (get_artifacts):", {k: list(v.keys()) for k, v in artifacts_1.items()})
    print("Artifacts (read_to_artifacts):", {k: list(v.keys()) for k, v in artifacts_2.items()})