#Example 4: author search

from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"authors": "Kleywegt", "limit": 5},
    )

    print("\nLoaded RCSBPDB backend using author search.")

    print("\nSearching loaded tables for author-related value:")
    dsi.search("Kleywegt")

    print("\nSearching loaded tables for mmCIF resource information:")
    dsi.search("mmCIF")

    if verbose:
        results = dsi.search("RCSBPDB", collection=True)
        if results:
            print(f"\nFound matches in {len(results)} result table(s).")
            for i, df in enumerate(results, start=1):
                print(f"\nResult {i}:")
                print(df.head())

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)