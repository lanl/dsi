# examples/ndp/2.process.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

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
    if verbose:
        print("Processed artifacts preview:")
        for table_name, table_data in tables.items():
            print(table_name)
            for col in table_data:
                print(f"  {col}: {table_data[col][:3]}")  # first 3 rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP process example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)