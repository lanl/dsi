"""
Example 5: Validate Zenodo file URLs and export tables to CSV.

This example demonstrates:

- loading Zenodo metadata
- validating resource download URLs
- exporting datasets/resources tables to CSV files

This example may make extra HTTP HEAD/GET requests because validate_urls()
checks each resource URL. The query limit is kept small for that reason.

Output files:

- examples/backends/zenodo/zenodo_datasets_export.csv
- examples/backends/zenodo/zenodo_resources_export.csv
"""

import warnings
from pathlib import Path

from urllib3.exceptions import InsecureRequestWarning

from dsi.backends.zenodo import Zenodo

warnings.simplefilter("ignore", InsecureRequestWarning)


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    output_dir = Path("examples/backends/zenodo")
    output_dir.mkdir(parents=True, exist_ok=True)

    datasets_out = output_dir / "zenodo_datasets_export.csv"
    resources_out = output_dir / "zenodo_resources_export.csv"

    zenodo = Zenodo(
        params={
            "keywords": "climate",
            "limit": 3,
        },
        verify_ssl=False,
        validate_resource_urls=False,
    )

    try:
        section("Loaded tables")
        zenodo.list()

        section("Validate resource URLs")
        valid_list = zenodo.validate_urls()
        print("Number of URLs checked:", len(valid_list))
        print("First few validity values:", valid_list[:10])

        section("Get updated resources table")
        resources = zenodo.get_table("resources")

        if resources.empty:
            print("No resources found.")
        else:
            print(
                resources[
                    [
                        "resource_id",
                        "name",
                        "download_url",
                        "url_valid",
                    ]
                ].head()
            )

        section("Export datasets and resources")
        datasets = zenodo.get_table("datasets")
        resources = zenodo.get_table("resources")

        datasets.to_csv(datasets_out, index=False)
        resources.to_csv(resources_out, index=False)

        print("Wrote:", datasets_out)
        print("Wrote:", resources_out)

    finally:
        zenodo.close()


if __name__ == "__main__":
    main()