# examples/ndp/ndp_developer/3.display.py
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()
    
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "water quality", "limit": 3}
    )
    
    if verbose:
        terminal.display("datasets", num_rows=5)
        
        terminal.display(
            "datasets",
            num_rows=3,
            display_cols=["title", "organization", "num_resources"]
        )
        
        table_names = list(terminal.list(collection=True))
        resource_tables = [t for t in table_names if t != "datasets"]
        
        if resource_tables:
            terminal.display(
                resource_tables[0],
                num_rows=5,
                display_cols=["resource_name", "format", "url"]
            )
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)