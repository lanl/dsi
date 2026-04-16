# examples/ndp/ndp_developer/1.load_basic.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()

    # Load NDP backend (read-only)
    terminal.load_module("backend", "NDP", "back-read", params={"keywords": "temperature", "limit": 5})

    backend = terminal.active_modules["back-read"][0]

    if verbose:
        artifacts = backend.summary()
        print("Datasets loaded:")
        print(artifacts)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)