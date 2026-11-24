from pathlib import Path

import pandas as pd

from dsi.dsi import DSI


def create_sample_csv(input_file: Path) -> None:
    sample_inputs = pd.DataFrame(
        {
            "identifier": [
                "1CBS",
                "10.2210/pdb4hhb/pdb",
            ]
        }
    )
    sample_inputs.to_csv(input_file, index=False)


def main():
    base_dir = Path(__file__).resolve().parent
    input_file = base_dir / "pdb_inputs.csv"
    output_csv = base_dir / "resources.csv"
    output_db = base_dir / "data.db"

    if not input_file.exists():
        create_sample_csv(input_file)
        print(f"\nCreated sample CSV input file: {input_file}")

    inputs_df = pd.read_csv(input_file)

    if "identifier" not in inputs_df.columns:
        raise RuntimeError("Input CSV file must contain an 'identifier' column.")

    identifiers = inputs_df["identifier"].dropna().astype(str).tolist()

    print("\nLoaded PDB IDs / DOIs from CSV:")
    for identifier in identifiers:
        print(f" - {identifier}")

    dsi = DSI(
        backend_name="RCSBPDB",
        silence_messages=True,
    )

    resources_df = dsi.query(
        {"identifiers": identifiers},
        collection=True,
    )
    resources_table = resources_df.attrs["table_name"]

    print(f"\nCached resource table: {resources_table}")

    print("\nFind cached rows for PDB entry 4HHB:")
    dsi.query("4HHB")

    print("\nSearch cached resource rows by file format:")
    dsi.query("format ~~ PDB")

    print("\nDisplay selected resource columns:")
    dsi.display(
        resources_table,
        display_cols=[
            "dataset_id",
            "name",
            "format",
            "download_url",
        ],
    )

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
        column
        for column in resource_columns
        if column in resources_df.columns
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

    if output_csv.exists():
        output_csv.unlink()

    print("\nExport resource table to CSV:")
    dsi.write(str(output_csv), "CSV", table_name=resources_table)
    print(f"Stored resource data in: {output_csv}")

    exported_df = pd.read_csv(output_csv)

    print("\nRead exported CSV with pandas:")
    with pd.option_context("display.max_columns", None, "display.width", None):
        print(exported_df.head())

    if output_db.exists():
        output_db.unlink()

    print("\nStore cached results in SQLite:")
    dsi.process("sqlite", str(output_db))
    print(f"Stored results in: {output_db}")

    dsi.close()


if __name__ == "__main__":
    main()