from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"authors": "Kleywegt", "limit": 5},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using author search.\n")

    print("Available tables:")
    dsi.list()

    datasets_df = dsi.get_table("datasets", collection=True)

    print("\nDatasets returned for author search:")
    columns = [
        "dataset_id",
        "doi",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_columns = [col for col in columns if col in datasets_df.columns]
    print(datasets_df[existing_columns])

    dsi.close()


if __name__ == "__main__":
    main()