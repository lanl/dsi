# examples/ndp/6.validate.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    backend.query_artifacts(query=None, kwargs={"keywords": "wildfire", "limit": 8})
    artifacts = backend.process_artifacts()

    if verbose:
        print("Datasets loaded:", list(artifacts["datasets"].keys()))
        print("Resources loaded:", list(artifacts["resources"].keys()))

    backend.validate_urls()

    resources = artifacts.get("resources", {})
    urls = resources.get("url", [])
    url_valid = resources.get("url_valid", [])

    if urls:
        for i, (u, v) in enumerate(zip(urls, url_valid)):
            if verbose or i < 5:
                print(f"{i+1}. {u} -> {'Valid' if v else 'Invalid'}")
    else:
        if verbose:
            print("No resources found. Try increasing 'limit' or check your query.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP URL validation example")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)