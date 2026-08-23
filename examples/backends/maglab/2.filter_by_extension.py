# examples/maglab/2.filter_by_extension.py
"""
Filter Maglab files by extension using include_ext and exclude_ext.
"""

from dsi.dsi import DSI

def main():
    node_id = "8r2b3"

    # Only include .tdms files
    print("=== include_ext='.tdms' ===")
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": node_id, "include_ext": ".tdms"}
    )
    tdms_count = dsi.num_tables(table_name="files")
    print(f"Files matching '.tdms': {tdms_count}")
    dsi.display("files", num_rows=5, display_cols=["name", "size_bytes"])
    dsi.close()

    # Exclude .tdms files - OSF nodes commonly contain other file types
    # (e.g. .png, .txt, .pdf) alongside the raw .tdms data files
    print("\n=== exclude_ext='.tdms' ===")
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": node_id, "exclude_ext": ".tdms"}
    )
    other_count = dsi.num_tables(table_name="files")
    print(f"Files NOT matching '.tdms': {other_count}")
    dsi.display("files", num_rows=5, display_cols=["name", "size_bytes"])
    dsi.close()

if __name__ == "__main__":
    main()
