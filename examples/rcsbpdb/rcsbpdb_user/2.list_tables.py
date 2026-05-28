from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="RCSBPDB",
        params={
            "pdb_id": "1CBS",
        },
    )

    print("\nLoaded RCSBPDB backend using PDB ID lookup.")

    print("\nInternal execution flow:")
    print("PDB ID -> Direct RCSB Data API lookup -> DSI tables")

    backend = dsi.main_backend_obj

    print(f"\nResolved identifiers: {backend.identifiers}")

    table_names = dsi.list(collection=True)

    print("\nListing RCSBPDB tables:")

    for name in table_names:
        df = dsi.get_table(name, collection=True)
        print(f" - {name}: ({df.shape[0]} rows, {df.shape[1]} cols)")

    datasets_df = dsi.get_table("datasets", collection=True)

    if not datasets_df.empty:
        print("\nCurated dataset preview:")

        preview_cols = [
            col for col in [
                "dataset_id",
                "title",
                "experimental_method",
                "release_date",
            ]
            if col in datasets_df.columns
        ]

        print(datasets_df[preview_cols].head())

    if verbose:
        print("\nVerbose table details:")

        for name in table_names:
            df = dsi.get_table(name, collection=True)

            print(f"\n{name}")
            print(f"Columns: {list(df.columns)}")

            if not df.empty:
                print(df.head())

        if "raw_metadata" in datasets_df.columns and not datasets_df.empty:
            print("\nFull metadata preservation:")
            print("datasets.raw_metadata stores curated metadata.")
            print("datasets.raw_metadata['full_metadata'] stores the full RCSB JSON response.")

            raw_meta = datasets_df.iloc[0]["raw_metadata"]

            if isinstance(raw_meta, dict):
                print("\nTop-level raw_metadata keys:")
                for key in raw_meta.keys():
                    print(f" - {key}")

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    main(verbose=args.verbose)