"""
Using display() with custom columns.
Shows how to customize which fields are displayed.
"""

from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "earth element",
            "limit": 10
        }
    )
    
    # ============================================
    # Display with specific columns (datasets)
    # ============================================
    
    # Minimal view - just essential info
    if verbose:
        print("\n=== Datasets - Minimal Columns ===")
    dsi.display('datasets', display_cols=['id', 'name', 'title', 'organization'])
    
    # Extended view - more metadata
    if verbose:
        print("\n=== Datasets - Extended Columns ===")
    dsi.display('datasets', display_cols=['id', 'title', 'organization', 'creator', 
                                          'created', 'modified', 'num_resources'])
    
    # All columns (except raw_*)
    if verbose:
        print("\n=== Datasets - All Columns ===")
    dsi.display('datasets', display_cols='all')
    
    # ============================================
    # Display with specific columns (resources)
    # ============================================
    
    # Minimal resource view
    if verbose:
        print("\n=== Resources - Minimal Columns ===")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 'url'])
    
    # Resource with metadata focus
    if verbose:
        print("\n=== Resources - Metadata Focus ===")
    dsi.display('resources', display_cols=['resource_id', 'resource_name', 'format', 
                                          'size', 'issue_date', 'dataset_title'])
    
    # All resource columns
    if verbose:
        print("\n=== Resources - All Columns ===")
    dsi.display('resources', display_cols='all', num_rows=5)
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)