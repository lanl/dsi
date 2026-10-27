# examples/navdat/4.search_tables.py
"""
Using search() to find values across all tables in NAVDAT.
"""

from dsi.dsi import DSI


def main():
    search_term = "walker"

    # Initialize NAVDAT backend
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Hekinian"}
    )

    print(f"\n=== Searching for '{search_term}' ===\n")

    # Get results as collection for analysis
    results = dsi.search(search_term, collection=True)

    if not results:
        print(f"No matches found for '{search_term}'")
        dsi.close()
        return

    print(f"Found matches in {len(results)} table(s)\n")

    for result_df in results:
        # Detect which table this is from
        if 'citationTitle' in result_df.columns:
            print("Citations Table:")
            essential_cols = ['citationTitle', 'citationAuthors', 'citationPublicationYear']
            available_cols = [col for col in essential_cols if col in result_df.columns]
            display_df = result_df[available_cols].drop_duplicates()
            print(display_df.to_string(index=False))
        elif 'sampleName' in result_df.columns:
            print("Samples Table:")
            essential_cols = ['sampleName', 'sampleId', 'sampleLat', 'sampleLon']
            available_cols = [col for col in essential_cols if col in result_df.columns]
            display_df = result_df[available_cols].drop_duplicates()
            print(display_df.to_string(index=False))
        else:
            print("Match (table/column-level):")
            print(result_df)
        print()

    dsi.close()


if __name__ == "__main__":
    main()