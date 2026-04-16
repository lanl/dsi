import argparse
from dsi.core import Terminal

def main(verbose=False):
    terminal = Terminal()

    # 1. Load NDP backend (ingest step)
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "energy", "limit": 10}
    )

    backend = terminal.active_modules["back-read"][0]

    # 2. Query step
    result = backend.query_artifacts(
        query="`num_resources` > 10",
        dict_return=True
    )

    # 3. OUTPUT STEP
    if verbose:
        print("\nQuery results (num_resources > 10):")
        print(f"Tables returned: {len(result)}")

        for table_name, table_data in result.items():
            print(f"\n[{table_name}]")

            for col, values in table_data.items():
                print(f"  {col}: {values}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NDP query example")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)