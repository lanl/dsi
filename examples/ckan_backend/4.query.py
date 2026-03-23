from dsi.core import Terminal

t = Terminal()

t.load_module("backend", "CKAN", "back-read")

backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(None, {"keywords": "energy", "limit": 20})

# Query datasets with at least one resource
result = backend.query_artifacts(
    "`num_resources` > 0",
    {}
)

print(result)