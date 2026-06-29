# examples/ndp/ndp_user/1.load_basic.py
"""
Basic NDP search using keywords.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize NDP backend with basic keyword search
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "climate", "limit": 10}
    )
    
    if verbose:
        print("\nTable List:")
        dsi.list()
        
        print("\nTable Summary:")
        dsi.summary()
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)