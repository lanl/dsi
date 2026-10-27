# examples/navdat/2.display_tables.py
"""
Using display() to view samples and citations data with various column
selections.
"""

from dsi.dsi import DSI


def main():
    # Initialize with a sample-name search
    dsi = DSI(
        backend_name="NAVDAT",
        params={"sampleNames": "(2.151) 12.40-12.85", "size": 5}
    )

    # Display samples with all columns (default)
    print("\nSamples Table (All Columns - Default):")
    dsi.display('samples', num_rows=5)

    # Display samples with minimal selection
    print("\nSamples (Minimal Selection):")
    dsi.display('samples', num_rows=5,
                 display_cols=['sampleName', 'sampleId', 'sampleLat', 'sampleLon'])

    # Display samples with extended metadata
    print("\nSamples (Extended Metadata):")
    dsi.display('samples', num_rows=5,
                 display_cols=['sampleName', 'sampleId', 'rootParent',
                                'groupDocCount', 'sampleDocCount'])

    dsi.close()

    # Separate query, since citations are best explored with an authors search
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Walker"}
    )

    if "citations" in dsi.list(collection=True):
        print("\nCitations Table (All Columns - Default):")
        dsi.display('citations', num_rows=3)

        print("\nCitations (Minimal Selection):")
        dsi.display('citations', num_rows=3,
                     display_cols=['citationTitle', 'citationAuthors',
                                    'citationPublicationYear'])

        print("\nCitations (Extended Metadata):")
        dsi.display('citations', num_rows=3,
                     display_cols=['citationTitle', 'citationContainerTitle',
                                    'citationDOIs', 'analysisType', 'methods'])

    dsi.close()


if __name__ == "__main__":
    main()