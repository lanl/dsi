# examples/navdat/6.write_and_process.py
"""
Export NAVDAT data to CSV and convert to a local SQLite database for
offline analysis.
"""

from dsi.dsi import DSI


def main():
    # Query citation data from NAVDAT (read-only)
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Walker"}
    )

    if "citations" not in dsi.list(collection=True):
        print("No citations table loaded - try a broader author search.")
        dsi.close()
        return

    # Export citations to CSV
    dsi.write(
        filename="navdat_citations.csv",
        writer_name="Csv",
        table_name="citations"
    )
    print("Exported citations to navdat_citations.csv")

    # Convert read-only NAVDAT data to a writable Sqlite backend
    dsi.process(
        backend_name="Sqlite",
        filename="navdat_walker.db"
    )
    dsi.close()
    print("Processed NAVDAT data into navdat_walker.db")

    # Load the newly created local database
    local_dsi = DSI(
        backend_name="Sqlite",
        filename="navdat_walker.db"
    )

    # Query the local database with real SQL (not supported directly on
    # the read-only NAVDAT backend)
    print("\nQuerying local SQLite copy:")
    local_dsi.query(
        "SELECT citationTitle, citationPublicationYear FROM citations "
        "ORDER BY citationPublicationYear DESC"
    )

    local_dsi.close()


if __name__ == "__main__":
    main()