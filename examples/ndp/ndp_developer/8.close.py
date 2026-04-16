# examples/ndp/ndp_developer/8.close.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate", "limit": 5}
    )
    
    backend = terminal.active_modules["back-read"][0]

    if verbose:
        print("\n=== Before close ===")
        print(f"Loaded: {backend._loaded}")
        print(f"Cached tables: {list(backend._cache.keys())}")
        summary = backend.summary()
        print(f"Number of tables: {len(summary)}")

    # Close resets the backend state
    backend.close()

    if verbose:
        print("\n=== After close ===")
        print(f"Loaded: {backend._loaded}")
        print(f"Cached tables: {list(backend._cache.keys())}")
        print(f"Resource tables: {backend._resource_tables}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP close/reset example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)