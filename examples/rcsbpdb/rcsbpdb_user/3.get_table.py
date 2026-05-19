from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "hemoglobin", "limit": 5},
    )

    print("\nGetting datasets table:")
    datasets_df = dsi.get_table("datasets", collection=True)
    print(f"Shape: {datasets_df.shape}")
    print(f"Columns: {list(datasets_df.columns)}")

    print("\nGetting resources table:")
    resources_df = dsi.get_table("resources", collection=True)
    print(f"Shape: {resources_df.shape}")
    print(f"Columns: {list(resources_df.columns)}")

    if verbose:
        print("\nFirst 3 dataset rows:")
        print(datasets_df.head(3))

        print("\nFirst 3 resource rows:")
        print(resources_df.head(3))

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)