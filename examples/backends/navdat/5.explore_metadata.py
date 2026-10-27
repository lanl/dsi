# examples/navdat/5.explore_metadata.py
"""
Explore available journals, analysis types, and publication years from
loaded citation data. Useful for discovering valid filter/context values
before making more targeted queries.

NOTE: the PetDB v4 API documents standalone vocabulary endpoints (e.g.
GET /v4/journals, GET /v4/analysisTypes) that would normally be the "right"
way to do this without first loading a sample dataset. As of 2026-08-24,
GET /v4/authors returned an empty body in live testing and was not further
investigated - see README "Known Limitations". This example works around
that by exploring values from an already-loaded query instead, which is
confirmed to work.
"""

from dsi.dsi import DSI


def main():
    print("Loading sample citation data...")
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Hekinian"}
    )

    if "citations" not in dsi.list(collection=True):
        print("No citations table loaded - try a broader author search.")
        dsi.close()
        return

    citations_df = dsi.get_table("citations", collection=True)

    print("\n=== Journals (citationContainerTitle) ===")
    print(citations_df['citationContainerTitle'].value_counts())

    print("\n=== Publication Years ===")
    years = citations_df['citationPublicationYear'].dropna()
    if not years.empty:
        print(f"Range: {int(years.min())} - {int(years.max())}")
        print(years.value_counts().sort_index())

    print("\n=== Analysis Types ===")
    print(citations_df['analysisType'].value_counts())

    print("\n=== Analytical Methods ===")
    all_methods = []
    for method_str in citations_df['methods']:
        if method_str:
            all_methods.extend(m.strip() for m in method_str.split(','))
    unique_methods = sorted(set(all_methods))
    print(f"Found {len(unique_methods)} unique methods: {unique_methods}")

    dsi.close()


if __name__ == "__main__":
    main()