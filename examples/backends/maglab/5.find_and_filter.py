# examples/maglab/5.find_and_filter.py
"""
Using find() and search() to filter and search loaded Maglab metadata.
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": "8r2b3"}
    )

    print("\nData Summary:")
    dsi.summary()

    # Find files larger than 1 MB (1,000,000 bytes)
    print("\nQuery: size_bytes > 1000000")
    results = dsi.find("size_bytes > 1000000", collection=True)
    if results is not None and len(results) > 0:
        print(f"\nFound {len(results)} large files:")
        print(results[["name", "size_bytes"]])

    # Search for "tdms" anywhere across all tables (table names, column
    # names, and cell values)
    print("\n=== Searching for 'tdms' ===")
    dsi.search("tdms")

    dsi.close()

if __name__ == "__main__":
    main()
