from dsi.dsi import DSI


def main(verbose=False):
    # Initialize WWPDB backend with query parameters.
    dsi = DSI(
        backend_name="WWPDB",
        params={"keywords": "hemoglobin", "limit": 5},
    )

    if verbose:
        print("\nAvailable tables:")
        dsi.list()

    dsi.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)