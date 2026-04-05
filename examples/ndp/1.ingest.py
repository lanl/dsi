# examples/ndp/1.ingest.py
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Simple ingestion
backend.query_artifacts(
    query=None,
    kwargs={"keywords": "data", "limit": 5}
)

print("Datasets loaded:")
for table_name, table_data in backend.process_artifacts().items():
    print(table_name, list(table_data.keys()))