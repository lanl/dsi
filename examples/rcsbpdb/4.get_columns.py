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
    output_csv = base_dir / "datasets.csv"
    output_db = base_dir / "data.db"

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
        params={"identifiers": identifiers + ["NOT_A_PDB_ID"]},
        silence_messages=True,
    )

    print("\nFind one loaded DOI:")
    dsi.find("doi = 10.2210/pdb4hhb/pdb")

    print("\nDisplay selected dataset columns:")
    dsi.display("datasets", display_cols=["title", "description"])

    resources_df = dsi.get_table("resources", collection=True)

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

    errors_df = dsi.get_table("errors", collection=True)

    print("\nErrors table:")
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        None,
        "display.max_colwidth",
        None,
    ):
        print(errors_df)

    if output_csv.exists():
        output_csv.unlink()

    print("\nExport datasets table to CSV:")
    dsi.write(str(output_csv), "CSV", table_name="datasets")
    print(f"Stored datasets table in: {output_csv}")

    if output_db.exists():
        output_db.unlink()

    print("\nStore results in SQLite:")
    dsi.process("sqlite", str(output_db))
    print(f"Stored results in: {output_db}")

    dsi.close()


if __name__ == "__main__":
    main()