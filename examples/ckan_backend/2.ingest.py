from dsi.core import Terminal

t = Terminal()

t.load_module("backend", "CKAN", "back-read")

backend = t.active_modules["back-read"][0]

backend.ingest_artifacts(
    artifacts=None,
    kwargs={
        "keywords": "data",
        "limit": 10
    }
)

print("Ingest successful.")