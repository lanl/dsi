# examples/ndp/ndp_developer/4.query.py
import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()

    # Load NDP backend (ingest step)
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "energy", "limit": 10}
    )

    backend = terminal.active_modules["back-read"][0]

    # Query with dict_return=True returns OrderedDict
    result = backend.query_artifacts(
        query="`num_resources` > 5",
        dict_return=True
    )

    if verbose:
        print("\n=== Query Results (num_resources > 5) ===")
        
        for table_name, table_data in result.items():
            print(f"\n[{table_name}]")
            print(f"  Columns: {list(table_data.keys())}")
            
            # Get number of rows
            num_rows = len(list(table_data.values())[0]) if table_data else 0
            print(f"  Total Rows: {num_rows}")
            
            # Print each row with its num_resources
            if 'num_resources' in table_data and num_rows > 0:
                print(f"\n  Rows with resource counts:")
                for i in range(num_rows):
                    title = table_data.get('title', ['N/A'] * num_rows)[i]
                    resources = table_data['num_resources'][i]
                    org = table_data.get('organization', ['N/A'] * num_rows)[i]
                    
                    # Truncate title if too long
                    title_display = title[:50] + '...' if len(str(title)) > 50 else title
                    
                    print(f"    Row {i+1}: {resources} resources - {title_display} ({org})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)