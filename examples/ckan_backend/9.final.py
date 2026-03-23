from dsi.core import Terminal

t = Terminal()

t.load_module("backend", "CKAN", "back-read")

backend = t.active_modules["back-read"][0]

# Ingest
backend.ingest_artifacts(None, {"keywords": "data", "limit": 5})

# Process
print(backend.process_artifacts({}))

# Query
print(backend.query_artifacts("num_resources > 0", {}))

# Find
print(backend.find("title", {}))

# Inspect
print(backend.inspect_artifacts({}))