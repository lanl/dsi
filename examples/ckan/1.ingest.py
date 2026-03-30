# examples/ckan/1.ingest.py
from dsi.core import Terminal

verbose = True

t = Terminal()
t.load_module("backend", "CKAN", "back-read")
backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(
    artifacts=None,
    kwargs={"keywords": "data", "limit": 10}
)

if verbose:
    print("CKAN back-read backend loaded successfully.")
    print("Datasets loaded:")
    for table_name, table_data in backend.process_artifacts().items():
        print(table_name, list(table_data.keys()))