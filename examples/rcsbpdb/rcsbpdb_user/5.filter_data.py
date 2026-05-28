# Example 5: experimental method search

from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"experimental_method": "X-RAY DIFFRACTION", "limit": 10},
    )

    datasets_df = dsi.get_table("datasets", collection=True)

    print("\nLoaded RCSBPDB backend using experimental_method search.")
    print("\nFiltering datasets by experimental method:")

    if "experimental_method" in datasets_df.columns:
        filtered = datasets_df[
            datasets_df["experimental_method"]
            .fillna("")
            .str.contains("X-RAY", case=False)
        ]

        print(f"Found {len(filtered)} X-RAY datasets")

        columns = ["dataset_id", "experimental_method", "title"]
        existing_columns = [col for col in columns if col in filtered.columns]
        print(filtered[existing_columns])

    print("\nFiltering datasets with at least one resource:")

    if "resource_count" in datasets_df.columns:
        with_resources = datasets_df[datasets_df["resource_count"] > 0]
        print(f"Found {len(with_resources)} datasets with resources")

        columns = ["dataset_id", "resource_count", "title"]
        existing_columns = [col for col in columns if col in with_resources.columns]
        print(with_resources[existing_columns])

    if verbose and "raw_metadata" in datasets_df.columns:
        print("\nRaw metadata is preserved in the raw_metadata column.")
        print("The full RCSB response is stored under raw_metadata['full_metadata'].")

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)