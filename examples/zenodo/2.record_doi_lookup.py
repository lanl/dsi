"""
Example 2: Lookup Zenodo records by record ID and DOI.

This example shows two precise lookup modes:
- record_id lookup
- DOI lookup

Both should return the same Zenodo record when the DOI corresponds
to the provided record ID.
"""

import warnings

from urllib3.exceptions import InsecureRequestWarning

from dsi.backends.zenodo import Zenodo


warnings.simplefilter("ignore", InsecureRequestWarning)


def show_backend_result(title, backend):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

    print("\nTable names:")
    print(backend.get_table_names())

    print("\nTables:")
    backend.list()

    datasets = backend.get_table("datasets")
    resources = backend.get_table("resources")

    print("\nDatasets:")
    print(datasets[["dataset_id", "doi", "title", "resource_count"]].head())

    print("\nResources:")
    if resources.empty:
        print("No resources found.")
    else:
        display_cols = [
            "resource_id",
            "dataset_id",
            "name",
            "format",
            "size",
            "download_url",
        ]
        print(resources[display_cols].head())


def main():
    record_backend = Zenodo(
        params={"record_id": "16537543"},
        verify_ssl=False,
    )

    doi_backend = Zenodo(
        params={"doi": "10.5281/zenodo.16537543"},
        verify_ssl=False,
    )

    try:
        show_backend_result("Record ID lookup", record_backend)
        show_backend_result("DOI lookup", doi_backend)

    finally:
        record_backend.close()
        doi_backend.close()


if __name__ == "__main__":
    main()