# examples/ndp/ndp_user/6.display_basic.py
"""
Using display() to view table data.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize with forest datasets
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "forest",
            "tags": ["lidar"],
            "limit": 20
        }
    )
    
    # Display datasets table
    if verbose:
        print("\nDatasets Table:")
    dsi.display('datasets')
    
    # Display with specific columns
    if verbose:
        print("\nDatasets (Selected Columns):")
    dsi.display('datasets', num_rows=3, display_cols=["name", "title", "tags"])
    
    # Display resources table
    if verbose:
        print("\nResources Table:")
    dsi.display('resources')
    
    # Display resources with specific columns
    if verbose:
        print("\nResources (Selected Columns):")
    dsi.display('resources', num_rows=5, display_cols=["resource_name", "dataset_title", "url"])
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)