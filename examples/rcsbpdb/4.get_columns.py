from pathlib import Path

import pandas as pd

from dsi.dsi import DSI


def create_sample_excel(input_file: Path) -> None:
    sample_inputs = pd.DataFrame(
        {
            "identifier": [
                "1CBS",
                "10.2210/pdb4hhb/pdb",
            ]
        }
    )
    sample_inputs.to_excel(input_file, index=False)


def main():
    base_dir = Path(__file__).resolve().parent
    input_file = base_dir / "pdb_inputs.xlsx"

    if not input_file.exists():
        create_sample_excel(input_file)
        print(f"\nCreated sample Excel input file: {input_file}")

    inputs_df = pd.read_excel(input_file)

    if "identifier" not in inputs_df.columns:
        raise RuntimeError("Input Excel file must contain an 'identifier' column.")

    identifiers = inputs_df["identifier"].dropna().astype(str).tolist()

    print("\nLoaded PDB IDs / DOIs from Excel:")
    for identifier in identifiers:
        print(f" - {identifier}")

    dsi = DSI(
        backend_name="RCSBPDB",
        params={"identifiers": identifiers},
        silence_messages=True,
    )

    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    errors_df = dsi.get_table("errors", collection=True)

    print("\nRetrieved datasets:")
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

    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        None,
        "display.max_colwidth",
        None,
    ):
        print(datasets_df[existing_dataset_columns])

    print("\nAssociated resource metadata and paths:")
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

    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        None,
        "display.max_colwidth",
        None,
    ):
        print(resources_df[existing_resource_columns])

    print("\nResource counts by dataset:")
    if not resources_df.empty and "dataset_id" in resources_df.columns:
        print(resources_df.groupby("dataset_id").size())
    else:
        print("No resources found.")

    if not errors_df.empty:
        print("\nErrors or skipped identifiers:")
        with pd.option_context(
            "display.max_columns",
            None,
            "display.width",
            None,
            "display.max_colwidth",
            None,
        ):
            print(errors_df)

    print("\nFull metadata location:")
    print("The curated fields are stored as columns in the datasets and resources tables.")
    print("The complete RCSB JSON response is stored in datasets.raw_metadata['full_metadata'].")

    dsi.close()


if __name__ == "__main__":
    main()