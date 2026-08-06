"""
Example 6: Use Zenodo through the user-facing DSI wrapper.

This example demonstrates the DSI wrapper integration for Zenodo.

Covered methods:
- DSI(...)
- list()
- list(collection=True)
- num_tables()
- get_table()
- display()
- summary()
- search()

Important:
This example intentionally does not call dsi.query() because the current
core.py cannot be changed in this workflow. dsi.query() expects a DataFrame
from artifact_handler(), but core.py currently calls query_artifacts()
without dict_return=False.
"""

import warnings

from urllib3.exceptions import InsecureRequestWarning

from dsi.dsi import DSI


warnings.simplefilter("ignore", InsecureRequestWarning)


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    dsi = DSI(
        backend_name="Zenodo",
        params={
            "keywords": "climate",
            "limit": 3,
        },
        verify_ssl=False,
    )

    try:
        section("List tables")
        dsi.list()

        section("List tables as collection")
        table_names = dsi.list(collection=True)
        print(table_names)

        section("Number of tables")
        dsi.num_tables()

        section("Get datasets as collection")
        datasets = dsi.get_table("datasets", collection=True)
        print(datasets.head())
        print("datasets shape:", datasets.shape)

        section("Get resources as collection")
        resources = dsi.get_table("resources", collection=True)
        print(resources.head())
        print("resources shape:", resources.shape)

        section("Display datasets")
        dsi.display(
            "datasets",
            num_rows=3,
            display_cols=[
                "dataset_id",
                "doi",
                "title",
                "resource_count",
                "usability_label",
            ],
        )

        section("Display resources")
        dsi.display(
            "resources",
            num_rows=5,
            display_cols=[
                "resource_id",
                "dataset_id",
                "name",
                "format",
                "size",
            ],
        )

        section("Summary")
        dsi.summary()

        section("Summary as collection")
        summaries = dsi.summary(collection=True)
        print(summaries)

        section("Search for Zenodo")
        dsi.search("Zenodo")

        section("DSI wrapper basic example complete")

    finally:
        dsi.close()


if __name__ == "__main__":
    main()