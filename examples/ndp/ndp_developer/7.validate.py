# examples/ndp/ndp_developer/7.validate.py
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
    
    # Process artifacts to get the tables
    artifacts = backend.process_artifacts()

    if verbose:
        print("\n=== Before Validation ===")
        print(f"Datasets loaded: {len(artifacts.get('datasets', {}).get('id', []))}")
        
        # Count total resources across all resource tables
        total_resources = sum(
            len(table.get('url', []))
            for name, table in artifacts.items()
            if name.startswith('resources_')
        )
        print(f"Total resources: {total_resources}")

    # Validate URLs (adds 'url_valid' column to resource tables)
    backend.validate_urls()

    if verbose:
        print("\n=== After Validation ===")
        for table_name in backend._resource_tables[:3]:  # Show first 3 resource tables
            table = artifacts.get(table_name, {})
            urls = table.get("url", [])
            url_valid = table.get("url_valid", [])
            
            print(f"\n{table_name}:")
            for i, (url, valid) in enumerate(zip(urls[:3], url_valid[:3])):  # First 3 URLs
                status = "✓ Valid" if valid else "✗ Invalid"
                print(f"  {i+1}. {status}: {url[:60]}...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP URL validation example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)