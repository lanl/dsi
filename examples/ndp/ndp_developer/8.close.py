# examples/ndp/ndp_developer/8.close.py
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
        print(f"\nBefore close: {len(backend._cache)} tables")
    
    backend.close()
    
    if verbose:
        print(f"After close: {len(backend._cache)} tables")
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)