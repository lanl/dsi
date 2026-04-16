import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()

    # 1. Load backend + trigger initial CKAN fetch (INGEST STEP)
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={
            "keywords": "climate",
            "tags": ["temperature", "humidity"],
            "organization": "NASA",
            "formats": ["CSV", "JSON"],
            "limit": 10
        }
    )

    backend = terminal.active_modules["back-read"][0]

    # 3. Process and print out data
    tables = backend.process_artifacts()

    if verbose:
        print("\nProcessed artifacts:\n")

        for table_name, table_data in tables.items():
            print(f"\n[{table_name}]")

            for col, values in table_data.items():
                print(f"  {col}: {values[:3]}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP process example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)