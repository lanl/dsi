# examples/ndp/ndp_developer/5.inspect.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "climate", "limit": 5})

    meta = backend.inspect_artifacts()
    artifacts_1 = backend.get_artifacts()
    artifacts_2 = backend.read_to_artifacts()

    if verbose:
        print("Inspect metadata summary:", meta)
        print("Artifacts (get_artifacts):", {k: list(v.keys()) for k, v in artifacts_1.items()})
        print("Artifacts (read_to_artifacts):", {k: list(v.keys()) for k, v in artifacts_2.items()})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP inspect example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)
    