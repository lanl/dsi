# examples/backends/denodo/4.find_and_search.py
"""
Two ways to look inside the loaded table:
  find()   - a condition on one column   (>, <, >=, <=, ==, !=, ~~ contains, ranges)
  search() - free text across every column

Neither makes another API call: both work on the table already in memory.
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Denodo",
        params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"},
    )

    print("\nViews whose name contains 'area':")
    dsi.find("name ~~ 'area'")

    # Take a database name from the result rather than hard-coding one, so the
    # example works against any Data Catalog.
    first_database = dsi.get_table("denodo_search_results", collection=True)["database_name"][0]

    print(f"\nViews in the database '{first_database}' (as a DataFrame):")
    rows = dsi.find(f"database_name == '{first_database}'", collection=True)
    print(f"{len(rows)} rows")

    print("\nFree-text search for 'waste' anywhere in the table:")
    dsi.search("waste")

    dsi.close()

if __name__ == "__main__":
    main()
