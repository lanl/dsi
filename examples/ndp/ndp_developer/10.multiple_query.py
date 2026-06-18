# examples/ndp/ndp_developer/
"""
Terminal interface - Multiple queries basic
"""
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    # Load with multiple queries - params is passed directly to load_module
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params=[
            {"keywords": "water quality", "limit": 10},
            {"keywords": "air quality", "limit": 10}
        ]
    )
    
    if verbose:
        print("\n=== Backend Status ===")
        terminal.num_tables()
        
        print("\n=== Table List ===")
        terminal.list()
        
        print("\n=== Summary ===")
        summary = terminal.summary(collection=True)
        print(f"Generated {len(summary) - 1} summary DataFrames")
    else:
        terminal.list()
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)