#Example 3: DOI lookup

from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"DOI": "10.2210/pdb1cbs/pdb"},
    )

    print("\nLoaded RCSBPDB backend using DOI lookup.")

    for table_name in ["datasets", "resources", "errors"]:
        print(f"\nGetting {table_name} table:")
        df = dsi.get_table(table_name, collection=True)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        if verbose and not df.empty:
            print(df.head(3))

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)