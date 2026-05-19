from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "hemoglobin", "limit": 5},
    )

    print("\nSearching for 'hemoglobin':")
    dsi.search("hemoglobin")
    # other examples "retinoic acid", "kinase", "DNA polymerase"
    print("\nSearching for 'X-RAY':")
    dsi.search("X-RAY")

    if verbose:
        results = dsi.search("mmCIF", collection=True)
        if results:
            print(f"\nFound matches in {len(results)} location(s)")
            for i, df in enumerate(results):
                print(f"\nResult {i + 1}:")
                print(df.head())

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)