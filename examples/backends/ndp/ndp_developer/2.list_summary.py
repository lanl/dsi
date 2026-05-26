# examples/ndp/ndp_developer/2.list_summary.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate energy", "limit": 10}
    )
    
    if verbose:
        terminal.list()
        print("\n")
        terminal.num_tables()
        print("\n")
        terminal.summary()
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)