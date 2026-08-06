"""
Example 3: Explore Zenodo DSI tables, summaries, display, and schema.

This example focuses on table inspection methods:
- get_table_names()
- list()
- num_tables()
- summary()
- display()
- get_schema()
"""

import warnings

from urllib3.exceptions import InsecureRequestWarning

from dsi.backends.zenodo import Zenodo


warnings.simplefilter("ignore", InsecureRequestWarning)


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    zenodo = Zenodo(
        params={
            "keywords": "battery materials",
            "limit": 3,
        },
        verify_ssl=False,
    )

    try:
        section("Table names")
        print(zenodo.get_table_names())

        section("List tables")
        zenodo.list()

        section("Number of tables")
        count = zenodo.num_tables()
        print("Returned count:", count)

        section("Summary of all tables")
        summary = zenodo.summary()
        print(summary)

        section("Summary of datasets table")
        print(zenodo.summary("datasets"))

        section("Summary of resources table")
        print(zenodo.summary("resources"))

        section("Display selected dataset columns")
        print(
            zenodo.display(
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
        )

        section("Display selected resource columns")
        print(
            zenodo.display(
                "resources",
                num_rows=5,
                display_cols=[
                    "resource_id",
                    "dataset_id",
                    "name",
                    "format",
                    "size",
                    "url_valid",
                ],
            )
        )

        section("Schema")
        print(zenodo.get_schema())

    finally:
        zenodo.close()


if __name__ == "__main__":
    main()