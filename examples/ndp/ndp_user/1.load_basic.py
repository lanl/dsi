# examples/ndp/ndp_user/1.load_basic.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi_instance = DSI(
        backend_name="NDP",
        keywords="temperature",
        limit=5
    )
    
    # ADD THESE DEBUG LINES:
    print(f"\nDEBUG: Backend object: {dsi_instance.main_backend_obj}")
    print(f"DEBUG: Backend loaded status: {dsi_instance.main_backend_obj._loaded}")
    print(f"DEBUG: Cache keys: {list(dsi_instance.main_backend_obj._cache.keys())}")
    print(f"DEBUG: Params used: {dsi_instance.main_backend_obj.params}")
    
    if verbose:
        print("\nAvailable tables:")
        dsi_instance.list()
    
    dsi_instance.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)