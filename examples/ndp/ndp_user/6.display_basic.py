"""
Using display() to show detailed information from tables.
Shows how to view data from datasets and resources tables.
"""

from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "forest",
            "tags": ["lidar"],
            "limit": 20
        }
    )
    
    # ============================================
    # Display datasets table (default view)
    # ============================================
    
    if verbose:
        print("\n=== Datasets Table (Default View) ===")
    dsi.display('datasets')
    
    # ============================================
    # Display with limited rows and specific columns
    # ============================================
    
    if verbose:
        print("\n=== Datasets Table (First 3 Rows) ===")
    dsi.display('datasets', num_rows=3, display_cols=["name", "title", "tags"])
    
    # ============================================
    # Display resources table
    # ============================================
    
    if verbose:
        print("\n=== Resources Table (Default View) ===")
    dsi.display('resources')
    
    # ============================================
    # Display with limited rows and specific columns
    # ============================================
    
    if verbose:
        print("\n=== Resources Table (First 5 Rows) ===")
    dsi.display('resources', num_rows=5, display_cols=["resource_name", "dataset_title", "url"])
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)