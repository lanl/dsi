# examples/ndp/ndp_developer/3.query.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "energy", "limit": 5})

    result = backend.query_in_memory(
        "`num_resources` > 10",
        {"table": "datasets", "dict_return": True}
    )

    if verbose:
        print("Query results (num_resources > 10):")
        for table_name, table_data in result.items():
            print(table_name, list(table_data))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)
    
# from dsi.core import DSI

# # 1) Initialize DSI with the NDP backend
# dsi = DSI(backend_name="NDP")

# # 2) Load a dataset into the backend (example: CSV via standard reader)
# dsi.read(
#     data_sources="traffic_data.csv",
#     reader_name="CSV",
#     table_name="traffic"
# )

# # 3) Run a SQL query on the NDP-backed dataset
# result = dsi.query("""
# SELECT road,
#        AVG(speed) AS avg_speed,
#        COUNT(*) AS observations
# FROM traffic
# GROUP BY road
# ORDER BY avg_speed DESC
# """, collection=True)

# print(result)