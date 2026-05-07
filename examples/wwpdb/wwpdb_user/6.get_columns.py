from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "retinoic acid", "limit": 5},
    )

    datasets_df = dsi.get_table("datasets", collection=True)

    useful_columns = [
        "dataset_id",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]

    existing_columns = [col for col in useful_columns if col in datasets_df.columns]

    print("\nSelected WWPDB dataset columns:")
    print(datasets_df[existing_columns])

    if verbose:
        print("\nAvailable dataset columns:")
        for col in datasets_df.columns:
            print(f"  - {col} ({datasets_df[col].dtype})")

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)