"""
Example 4: Local filtering and find methods.

This example demonstrates local table filtering after Zenodo data is loaded.

Covered methods:

- query_artifacts() with pandas-style filters
- find_table()
- find_column()
- find_cell(), which returns matching rows as ValueObjects
- find()
- find_relation() with split arguments
- find_relation() with one-string conditions
- find_relation() with API-backed lookup/search

Note:
query_artifacts() here is used directly on the Zenodo backend.
It does not use DSI.query().
"""

import warnings

from urllib3.exceptions import InsecureRequestWarning

from dsi.backends.zenodo import Zenodo

warnings.simplefilter("ignore", InsecureRequestWarning)


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_value_objects(values, limit=5):
    if not values:
        print("No matches.")
        return

    for value in values[:limit]:
        if hasattr(value, "to_dict"):
            print(value.to_dict())
        else:
            print(value)

    if len(values) > limit:
        print(f"... showing {limit} of {len(values)} matches")


def show_tables_result(tables):
    print("Returned object type:", type(tables))
    if isinstance(tables, dict):
        print("Table names:", list(tables.keys()))


def main():
    zenodo = Zenodo(
        params={
            "keywords": "climate",
            "limit": 5,
        },
        verify_ssl=False,
    )

    try:
        section("Loaded tables")
        zenodo.list()

        section("Filter datasets where resource_count >= 0")
        dataset_filter = zenodo.query_artifacts(
            "resource_count >= 0",
            dict_return=False,
        )
        print(dataset_filter.head())
        print("shape:", dataset_filter.shape)

        section("Filter resources where size >= 0")
        resource_filter = zenodo.query_artifacts(
            "size >= 0",
            dict_return=False,
        )
        print(resource_filter.head())
        print("shape:", resource_filter.shape)

        section("Find table containing 'data'")
        show_value_objects(zenodo.find_table("data"))

        section("Find columns containing 'doi'")
        show_value_objects(zenodo.find_column("doi"))

        section("Find rows with cells containing 'Zenodo'")
        show_value_objects(zenodo.find_cell("Zenodo"))

        section("Find all matches for 'doi'")
        show_value_objects(zenodo.find("doi"))

        section("find_relation split args: datasets resource_count >= 0")
        relation_matches = zenodo.find_relation("resource_count", ">= 0")
        show_value_objects(relation_matches)

        section("find_relation split args: resources size >= 0")
        relation_matches = zenodo.find_relation("size", ">= 0")
        show_value_objects(relation_matches)

        section("find_relation split args: title contains climate")
        relation_matches = zenodo.find_relation("title", "~ climate")
        show_value_objects(relation_matches)

        section("find_relation one-string: resource_count >= 0")
        relation_matches = zenodo.find_relation("resource_count >= 0")
        show_value_objects(relation_matches)

        section("find_relation one-string: size >= 0")
        relation_matches = zenodo.find_relation("size >= 0")
        show_value_objects(relation_matches)

        section("find_relation one-string: title contains climate")
        relation_matches = zenodo.find_relation("title ~ climate")
        show_value_objects(relation_matches)

        section("API-backed find_relation: record_id lookup")
        tables = zenodo.find_relation("record_id = 16537543")
        show_tables_result(tables)
        zenodo.list()

        section("API-backed find_relation: DOI lookup")
        tables = zenodo.find_relation("doi = 10.5281/zenodo.16537543")
        show_tables_result(tables)
        zenodo.list()

        section("API-backed find_relation: keyword search")
        tables = zenodo.find_relation("keywords ~ climate", limit=3)
        show_tables_result(tables)
        zenodo.list()

    finally:
        zenodo.close()


if __name__ == "__main__":
    main()