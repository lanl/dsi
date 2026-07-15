# examples/ndp/ndp_developer/6.get_columns.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(backend_name="NDP", keywords="climate", limit=10)
    
    df = dsi.get_table("datasets", collection=True)
    
    # Select columns
    subset = df[['title', 'organization', 'num_resources']]
    print("\nDataset summary:")
    print(subset)
    
    if verbose:
        print("\n\nAvailable columns:")
        for col in df.columns:
            print(f"  - {col} ({df[col].dtype})")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)