from dsi.core import Terminal
import pandas as pd

t = Terminal()

t.load_module("backend", "CKAN", "back-read")

backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(None, {"keywords": "science", "limit": 5})

tables = backend.process_artifacts({})

print("Tables loaded:")
for name, table in tables.items():
    print(name, list(table.keys()))