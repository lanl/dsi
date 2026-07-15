# examples/ndp/ndp_developer/1.load_basic.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "temperature", "limit": 5}
    )
    
    if verbose:
        print("\nAvailable tables:")
        terminal.list()
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)