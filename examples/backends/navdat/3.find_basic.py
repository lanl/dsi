# examples/navdat/3.find_basic.py
"""
Basic find operations on NAVDAT citation and sample data.
Demonstrates practical string-based and numeric queries.
"""

from dsi.dsi import DSI


def main():
    # Initialize NAVDAT backend with an author search (populates citations)
    dsi = DSI(
        backend_name="NAVDAT",
        params={"authors": "Hekinian"}
    )

    print("\nData Summary:")
    dsi.summary()

    # Numeric comparison - citations published after 1990
    print("\nQuery: citationPublicationYear > 1990")
    dsi.find("citationPublicationYear > 1990")

    # Same query but return DataFrame
    print("\nSame query with collection=True:")
    results = dsi.find("citationPublicationYear > 1990", collection=True)
    if results is not None and len(results) > 0:
        print(f"Returned DataFrame with {len(results)} rows and {len(results.columns)} columns")

    # Partial string match - journals containing 'PETROL'
    print("\nQuery: citationContainerTitle ~~ 'PETROL'")
    results = dsi.find("citationContainerTitle ~~ 'PETROL'", collection=True)
    if results is not None and len(results) > 0:
        print(f"\nFound {len(results)} matches:")
        print(results[['citationTitle', 'citationContainerTitle', 'citationPublicationYear']].head())

    # Find citations using a specific analysis method
    print("\nQuery: methods ~~ 'MICROPROBE'")
    results = dsi.find("methods ~~ 'MICROPROBE'", collection=True)
    if results is not None and len(results) > 0:
        print(f"\nFound {len(results)} citations using microprobe methods")
        print(results[['citationTitle', 'methods']].head())

    dsi.close()


if __name__ == "__main__":
    main()