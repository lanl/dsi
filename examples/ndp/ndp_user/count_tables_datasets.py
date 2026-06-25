# examples/ndp/ndp_user/6.count_tables_datasets.py
"""
Demonstrates num_tables() vs num_datasets() in NDP backend.
"""

from dsi.dsi import DSI

def main(verbose=False):
    # Initialize with 8 datasets
    dsi = DSI(
        backend_name="NDP",
        params={"keyword": "energy", "limit": 10}
    )
    
    # Show logical table count
    print("\nLogical tables (datasets + resources):")
    dsi.num_tables()
    
    # Show dataset count
    print("\nNumber of datasets retrieved:")
    dsi.num_datasets()
    
    if verbose:
        print("\nDetailed breakdown:")
        dsi.list()
        
        # Show dataset titles
        datasets_df = dsi.get_table("datasets", collection=True)
        print(f"\nDataset titles ({len(datasets_df)} total):")
        for idx, title in enumerate(datasets_df['title'], 1):
            print(f"  {idx}. {title}")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Demonstrate num_tables() vs num_datasets()"
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)