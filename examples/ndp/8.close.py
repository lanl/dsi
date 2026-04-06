# examples/ndp/8.close.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "climate", "limit": 5})

    backend.close()

    if verbose:
        print("Backend state after close:", backend.inspect_artifacts())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP close/reset example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)