from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "hemoglobin", "limit": 10},
    )

    datasets_df = dsi.get_table("datasets", collection=True)

    print("\nFiltering datasets by experimental method:")
    if "experimental_method" in datasets_df.columns:
        filtered = datasets_df[
            datasets_df["experimental_method"].fillna("").str.contains("X-RAY", case=False)
        ]
        print(f"Found {len(filtered)} X-RAY datasets")
        print(filtered[["dataset_id", "experimental_method", "title"]])

    print("\nFiltering datasets with at least one resource:")
    if "resource_count" in datasets_df.columns:
        with_resources = datasets_df[datasets_df["resource_count"] > 0]
        print(f"Found {len(with_resources)} datasets with resources")
        print(with_resources[["dataset_id", "resource_count", "title"]])

    if verbose:
        print("\nRelease dates:")
        if "release_date" in datasets_df.columns:
            print(datasets_df[["dataset_id", "release_date", "title"]])

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)