# examples/ndp/2.process.py
from dsi.core import Terminal

t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Ingest with multiple filters
backend.query_artifacts(
    query=None,
    kwargs={
        "keywords": "climate",
        "tags": ["temperature", "humidity"],
        "organization": "NASA",
        "formats": ["CSV", "JSON"],
        "limit": 10
    }
)

tables = backend.process_artifacts()
print("Processed artifacts preview:")
for table_name, table_data in tables.items():
    print(table_name)
    for col in table_data:
        print(f"  {col}: {table_data[col][:3]}")  # show first 3 rows