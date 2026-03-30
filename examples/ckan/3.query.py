# examples/ckan/3.query.py
from dsi.core import Terminal

verbose = True

t = Terminal()
t.load_module("backend", "CKAN", "back-read")
backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(None, {"keywords": "energy", "limit": 20})

# Query datasets with at least one resource
result = backend.query_artifacts("`num_resources` > 10", {"table": "datasets", "dict_return": True})

if verbose:
    print("Query results (num_resources > 10):")
    for table_name, table_data in result.items():
        print(table_name, list(table_data))