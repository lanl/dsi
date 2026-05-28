# Example 6: Excel-driven DOI/PDB ID workflow

from pathlib import Path

import pandas as pd
import requests

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


def download_file(url: str, output_path: Path) -> None:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    output_path.write_bytes(response.content)


def main(verbose=False):
    base_dir = Path(__file__).resolve().parent
    input_file = base_dir / "pdb_inputs.xlsx"
    download_dir = base_dir / "downloaded_rcsbpdb_files"
    download_dir.mkdir(exist_ok=True)

    if not input_file.exists():
        create_sample_excel(input_file)
        print(f"\nCreated sample Excel input file: {input_file}")

    inputs_df = pd.read_excel(input_file)

    if "identifier" not in inputs_df.columns:
        raise RuntimeError("Input Excel file must contain an 'identifier' column.")

    identifiers = inputs_df["identifier"].dropna().astype(str).tolist()

    print("\nLoaded identifiers from Excel:")
    for identifier in identifiers:
        print(f" - {identifier}")

    dsi = DSI(
        backend_name="RCSBPDB",
        params={"identifiers": identifiers},
    )

    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    errors_df = dsi.get_table("errors", collection=True)

    print("\nRetrieved curated datasets table:")
    selected_columns = [
        "dataset_id",
        "title",
        "experimental_method",
        "release_date",
        "resource_count",
    ]
    existing_columns = [col for col in selected_columns if col in datasets_df.columns]
    print(datasets_df[existing_columns])

    print("\nDownloading associated resource files:")

    if resources_df.empty:
        print("No downloadable resources found.")
    else:
        for _, row in resources_df.iterrows():
            url = row.get("download_url")
            name = row.get("name") or f"{row.get('resource_id', 'resource')}.dat"

            if not isinstance(url, str) or not url.strip():
                continue

            output_path = download_dir / name

            try:
                download_file(url, output_path)
                print(f"Downloaded: {output_path}")
            except Exception as exc:
                print(f"Failed to download {url}: {exc}")

    if not errors_df.empty:
        print("\nErrors or skipped identifiers:")
        print(errors_df)

    if verbose:
        print("\nAvailable dataset columns:")
        for col in datasets_df.columns:
            print(f" - {col}")

        if "raw_metadata" in datasets_df.columns and not datasets_df.empty:
            print("\nFull metadata location:")
            print("datasets.raw_metadata contains curated raw metadata.")
            print("datasets.raw_metadata['full_metadata'] contains the full RCSB JSON response.")

        print("\nLocal outputs:")
        print(f" - Excel input: {input_file}")
        print(f" - Download directory: {download_dir}")

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)