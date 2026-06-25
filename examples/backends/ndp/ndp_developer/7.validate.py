# examples/ndp/ndp_developer/7.validate.py
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
        print("\nValidating URLs...")
    
    backend.validate_urls()
    
    if verbose:
        artifacts = backend.process_artifacts()
        for table_name in backend._resource_tables[:2]:
            table = artifacts.get(table_name, {})
            urls = table.get("url", [])
            valid = table.get("url_valid", [])
            
            print(f"\n{table_name}:")
            print(f"  Total URLs: {len(urls)}")
            print(f"  Valid: {sum(valid)}/{len(valid)}")
    
    terminal.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)