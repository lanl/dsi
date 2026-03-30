# examples/ckan/2.process.py
from dsi.core import Terminal

verbose = False

t = Terminal()
t.load_module("backend", "CKAN", "back-read")
backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(None, {"keywords": "science", "limit": 5})
tables = backend.process_artifacts()

if verbose:
    print("Processed artifacts preview:")
    for table_name, table_data in tables.items():
        print(table_name)
        for col in table_data:
            print(f"  {col}: {table_data[col]}")