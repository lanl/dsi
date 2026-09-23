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

    print("\nViews in one database (as a DataFrame):")
    rows = dsi.find("database_name == 'dataportal'", collection=True)
    print(f"{len(rows)} rows")

    print("\nFree-text search for 'waste' anywhere in the table:")
    dsi.search("waste")

    dsi.close()

if __name__ == "__main__":
    main()
