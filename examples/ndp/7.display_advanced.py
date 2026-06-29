# examples/ndp/ndp_user/7.display_advanced.py
"""
Advanced display options with custom column selection.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize with earth datasets
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "earth element", "limit": 10}
    )
    
    # Minimal dataset view
    if verbose:
        print("\nDatasets (Minimal):")
    dsi.display('datasets', display_cols=['id', 'name', 'title', 'organization'])
    
    # Extended dataset view with metadata
    if verbose:
        print("\nDatasets (Extended):")
    dsi.display('datasets', display_cols=['id', 'title', 'organization', 'creator', 
                                          'created', 'modified', 'num_resources'])
    
    # All dataset columns
    if verbose:
        print("\nDatasets (All Columns):")
    dsi.display('datasets', display_cols='all')
    
    # Minimal resource view
    if verbose:
        print("\nResources (Minimal):")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 'url'])
    
    # Resource view with metadata
    if verbose:
        print("\nResources (With Metadata):")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 
                                          'size', 'issue_date', 'dataset_title'])
    
    # All resource columns
    if verbose:
        print("\nResources (All Columns):")
    dsi.display('resources', display_cols='all', num_rows=5)
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)