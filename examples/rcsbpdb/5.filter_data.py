from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"experimental_method": "SOLUTION NMR", "limit": 25},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using SOLUTION NMR search.")

    datasets_df = dsi.get_table("datasets", collection=True)

    print("\nDataset count:")
    print(len(datasets_df))

    print("\nExperimental methods found:")
    if "experimental_method" in datasets_df.columns:
        print(datasets_df["experimental_method"].value_counts())

    print("\nResource count distribution:")
    if "resource_count" in datasets_df.columns:
        print(datasets_df["resource_count"].value_counts().sort_index())

    print("\nTop datasets by resource count:")
    columns = [
        "dataset_id",
        "resource_count",
        "experimental_method",
        "title",
    ]
    existing_columns = [col for col in columns if col in datasets_df.columns]

    print(
        datasets_df[existing_columns]
        .sort_values("resource_count", ascending=False)
        .head(10)
    )

    dsi.close()


if __name__ == "__main__":
    main()