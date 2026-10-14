# examples/backends/denodo/8.load_columns.py
"""
Load the column and property tables for a specific set of views.

Unlike the other examples, this one fetches per view: one call each, about
1.3 seconds. The scope is therefore required -- harvesting the whole
catalogue would take over an hour. Requires the settings described in
README.md ("Configuration").
"""

from dsi.dsi import DSI

def main():
    # Three ways to scope it. A keyword is the usual one.
    dsi = DSI(backend_name="Denodo", params={"columns": "your_keyword"})


    #   params={"columns": {"database": "your_database"}}
    #   params={"columns": ["your_database.your_view"]}

    print("\nColumns:")
    dsi.summary("denodo_columns")

    print("\nCustom properties:")
    dsi.summary("denodo_properties")

    columns = dsi.get_table("denodo_columns", collection=True)
    properties = dsi.get_table("denodo_properties", collection=True)

    views = columns["view_name"].nunique()
    print(f"\n{len(columns)} columns and {len(properties)} properties "
          f"across {views} view(s)")

    # Column descriptions are rare in practice -- worth checking before
    # relying on them.
    described = columns["description"].notna().sum()
    print(f"Columns carrying a description: {described} of {len(columns)}")

    # Views whose details could not be fetched, as (database, view) pairs.
    failed = dsi.main_backend_obj.failed_views
    print(f"Views that failed: {len(failed)}")

    dsi.close()

if __name__ == "__main__":
    main()
