from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"DOI": "10.2210/pdb1cbs/pdb"},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using DOI lookup.")

    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    errors_df = dsi.get_table("errors", collection=True)

    print("\nDatasets:")
    dataset_columns = [
        "dataset_id",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_dataset_columns = [
        col for col in dataset_columns if col in datasets_df.columns
    ]
    print(datasets_df[existing_dataset_columns])

    print("\nResources:") 
    resource_columns = [
        "resource_id",
        "dataset_id",
        "name",
        "format",
        "download_url",
    ]
    existing_resource_columns = [
        col for col in resource_columns if col in resources_df.columns
    ]
    print(resources_df[existing_resource_columns].head())

    if not errors_df.empty:
        print("\nErrors:")
        print(errors_df)

    dsi.close()


if __name__ == "__main__":
    main()