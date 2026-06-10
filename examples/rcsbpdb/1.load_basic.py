
## 1.load_basic.py

from dsi.dsi import DSI
def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"keywords": "genomics", "limit": 10},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using keyword search.\n")

    print("Available tables:")
    dsi.list()

    datasets_df = dsi.get_table("datasets", collection=True)

    print("\nDatasets table preview:")
    columns = [
        "dataset_id",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_columns = [col for col in columns if col in datasets_df.columns]
    print(datasets_df[existing_columns].head())
    dsi.close()

if __name__ == "__main__":
    main()