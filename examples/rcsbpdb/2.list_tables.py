from dsi.dsi import DSI


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"pdb_id": "1CBS"},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using PDB ID lookup.\n")

    print("Available tables:")
    dsi.list()

    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    errors_df = dsi.get_table("errors", collection=True)

    print("\nTable dimensions:")
    print(f"datasets: {datasets_df.shape[0]} rows, {datasets_df.shape[1]} cols")
    print(f"resources: {resources_df.shape[0]} rows, {resources_df.shape[1]} cols")
    print(f"errors: {errors_df.shape[0]} rows, {errors_df.shape[1]} cols")

    print("\nDatasets preview:")
    columns = ["dataset_id", "title", "experimental_method", "release_date"]
    existing_columns = [col for col in columns if col in datasets_df.columns]
    print(datasets_df[existing_columns])

    dsi.close()


if __name__ == "__main__":
    main()