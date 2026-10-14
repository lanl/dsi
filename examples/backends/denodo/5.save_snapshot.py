# examples/backends/denodo/5.save_snapshot.py
"""
Save a search result as a local SQLite snapshot, then query it with SQL.

After this, the data can be explored offline: no Denodo, no token.
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Denodo",
        params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"},
    )

    print("\nWhat was loaded:")
    dsi.list()

    dsi.process(backend_name="Sqlite", filename="denodo_cui.db")
    print("\nSaved denodo_cui.db")
    dsi.close()

    # From here on: plain SQL against the local file
    local = DSI(backend_name="Sqlite", filename="denodo_cui.db")

    print("\nViews tagged with Waste:")
    local.query(
        "SELECT name, database_name, categories "
        "FROM denodo_search_results "
        "WHERE tags LIKE '%Waste%' "
        "ORDER BY name LIMIT 10"
    )

    local.close()

if __name__ == "__main__":
    main()
