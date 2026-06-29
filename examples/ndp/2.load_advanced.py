# examples/ndp/ndp_user/2.load_advanced.py
"""
Advanced NDP search with multiple filter parameters.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize NDP backend with advanced search parameters
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "tree state park",
            "organization": "BurnPro3D",
            "group": "data_hub_cc_wstc",
            "tags": ["boundaries", "burn-units"],
            "formats": ["GeoJSON"],
            "limit": 15
        }
    )
    
    if verbose:
        print("\nTable List:")
        dsi.list()
        
        print("\nTable Summary:")
        dsi.summary()
        
        # Show just the formats and resource names
        dsi.display("resources", display_cols=["resource_name", "format"])
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)
