# examples/ndp/ndp_user/1.load_basic.py
from dsi.dsi import DSI

def main(verbose=False):
    # Initialize NDP backend with search parameters
    dsi = DSI(
        backend_name="NDP",
        keywords="temperature",  # Search term
        limit=5                  # Maximum datasets to retrieve
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