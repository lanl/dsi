from dsi.dsi import DSI


def main(verbose=False):
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "retinoic acid", "limit": 5},
    )

    print("\nListing WWPDB tables:")
    dsi.list()

    if verbose:
        table_names = dsi.list(collection=True)
        print(f"\nFound {len(table_names)} tables:")
        for name in table_names:
            print(f"  - {name}")

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)