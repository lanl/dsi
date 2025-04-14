# examples/core/user_api.py
from dsi.core import DSI

a = DSI()
a.schema('../data/example_schema.json')
a.toml1(["../data/results.toml", "../data/results1.toml"])
a.sqlbackend('data.db')
a.ingest()
