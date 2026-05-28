from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="RCSBPDB",
        params={
            "keywords": "hemoglobin",
            "limit": 5,
        },
    )

    print("\nLoaded RCSBPDB backend using keyword search.")

    print("\nInternal execution flow:")
    print("keywords -> RCSB Search API -> PDB IDs -> RCSB Data API -> DSI tables")

    backend = dsi.main_backend_obj

    print(f"\nResolved PDB IDs: {backend.identifiers}")

    table_names = dsi.list(collection=True)

    print("\nAvailable tables:")
    for name in table_names:
        df = dsi.get_table(name, collection=True)
        print(f" - {name}: {df.shape[0]} rows, {df.shape[1]} cols")

    datasets_df = dsi.get_table("datasets", collection=True)

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

    if not datasets_df.empty:
        print(datasets_df[preview_cols].head())
    else:
        print("No datasets returned.")

    if verbose:
        print("\nVerbose details:")

        print("\nDatasets schema columns:")
        for col in datasets_df.columns:
            print(f" - {col}")

        if "raw_metadata" in datasets_df.columns and not datasets_df.empty:
            print("\nRaw metadata preservation:")
            print("datasets.raw_metadata contains curated raw metadata.")
            print("datasets.raw_metadata['full_metadata'] contains the full RCSB JSON response.")

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