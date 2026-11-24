import pandas as pd

from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        silence_messages=True,
    )

    datasets_df = dsi.query(
        {"keywords": "hemoglobin", "limit": 5},
        collection=True,
    )
    datasets_table = datasets_df.attrs["table_name"]

    print("\nDataset-discovery table preview:")

    dataset_columns = [
        "dataset_id",
        "doi",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_dataset_columns = [
        column
        for column in dataset_columns
        if column in datasets_df.columns
    ]

    with pd.option_context("display.max_columns", None, "display.width", None):
        print(datasets_df[existing_dataset_columns].head())

    if datasets_df.empty:
        print("\nNo datasets were returned; cannot continue to resource retrieval.")
        dsi.close()
        return

    selected_pdb_id = datasets_df.iloc[0]["dataset_id"]

    resources_df = dsi.query(
        {"pdb_id": selected_pdb_id},
        collection=True,
    )
    resources_table = resources_df.attrs["table_name"]

    print(f"\nSelected PDB entry: {selected_pdb_id}")
    print(f"Cached resource table: {resources_table}")

    resource_columns = [
        "resource_id",
        "dataset_id",
        "name",
        "format",
        "resource_type",
        "download_url",
    ]
    existing_resource_columns = [
        column
        for column in resource_columns
        if column in resources_df.columns
    ]

    print("\nResources table preview:")
    with pd.option_context("display.max_columns", None, "display.width", None):
        print(resources_df[existing_resource_columns].head())

    print("\nAll cached tables:")
    dsi.list()

    print("\nSummary of the discovery table:")
    dsi.summary(datasets_table)

    dsi.close()


if __name__ == "__main__":
    main()