import pandas as pd
from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"keywords": "genomics", "limit": 10},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using keyword search.")

    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    errors_df = dsi.get_table("errors", collection=True)

    print("\nDatasets table preview:")
    dataset_columns = [
        "dataset_id",
        "doi",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_dataset_columns = [
        col for col in dataset_columns if col in datasets_df.columns
    ]

    with pd.option_context("display.max_columns", None, "display.width", None):
        print(datasets_df[existing_dataset_columns].head())

    print("\nResources table preview:")
    resource_columns = [
        "resource_id",
        "dataset_id",
        "name",
        "format",
        "resource_type",
        "download_url",
    ]
    existing_resource_columns = [
        col for col in resource_columns if col in resources_df.columns
    ]

    with pd.option_context("display.max_columns", None, "display.width", None):
        print(resources_df[existing_resource_columns].head())

    print("\nErrors table preview:")
    if errors_df.empty:
        print("No errors returned for this query.")
    else:
        with pd.option_context("display.max_columns", None, "display.width", None):
            print(errors_df.head())

    dsi.close()


if __name__ == "__main__":
    main()