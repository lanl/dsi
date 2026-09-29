# examples/backends/denodo/6.load_databases.py
"""
Load the database table: one row per database in the Data Catalog.

Two calls, no matter how many databases exist: one for the databases and
their descriptions, one for the view counts. Requires the settings
described in README.md ("Configuration").
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(backend_name="Denodo", params={"databases": []})

    print("\nTable Summary:")
    dsi.summary("denodo_databases")

    print("\nEvery database:")
    dsi.display("denodo_databases")

    dsi.close()

    # One database only:  params={"databases": ["<a name from db_name>"]}
    # An unknown name is rejected before any metadata is fetched.


if __name__ == "__main__":
    main()
