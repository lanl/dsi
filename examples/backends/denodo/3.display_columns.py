# examples/backends/denodo/3.display_columns.py
"""
Viewing the table: all 17 columns, or a chosen subset.
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Denodo",
        params={"keywords": "CUI", "search_in": ["properties"], "match": "substring"},
    )

    print("\nAll columns (wide):")
    dsi.display("denodo_search_results", num_rows=3)

    print("\nThe columns most people want:")
    dsi.display(
        "denodo_search_results",
        num_rows=10,
        display_cols=["name", "database_name", "description", "categories", "tags"],
    )

    # The table as a DataFrame, for pandas work
    df = dsi.get_table("denodo_search_results", collection=True)
    print(f"\nShape: {df.shape}")
    print("\nViews per database:")
    print(df["database_name"].value_counts())

    dsi.close()

if __name__ == "__main__":
    main()
