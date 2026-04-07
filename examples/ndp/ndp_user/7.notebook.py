# examples/ndp/ndp_developer/7.notebook.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "wildfire", "limit": 5})

    if verbose:
        backend.notebook()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP notebook preview example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)