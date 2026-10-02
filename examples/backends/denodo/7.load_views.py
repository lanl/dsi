# examples/backends/denodo/7.load_views.py
"""
Load the view table: one row per view in the Data Catalog.

Built from the same search endpoint the search examples use, so the whole
catalogue arrives in roughly 45 requests rather than one call per view.
Requires the settings described in README.md ("Configuration").
"""

from dsi.dsi import DSI

def main():
    # True means every view in the catalogue; pass a string to narrow it.
    dsi = DSI(backend_name="Denodo", params={"views": True})

    print("\nTable Summary:")
    dsi.summary("denodo_views")

    print("\nFirst rows:")
    dsi.display("denodo_views", num_rows=5,
                display_cols=["view_name", "db_name", "categories", "documentation_url"])

    # Views whose description carries a documentation link.
    views = dsi.get_table("denodo_views", collection=True)
    with_docs = views["documentation_url"].notna().sum()
    print(f"\nViews with a documentation URL: {with_docs} of {len(views)}")

    dsi.close()

    # One subject area only:  params={"views": "your_keyword"}

if __name__ == "__main__":
    main()
