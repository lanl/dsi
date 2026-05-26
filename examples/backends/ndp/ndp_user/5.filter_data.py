# examples/ndp/ndp_user/5.filter_data.py
from dsi.dsi import DSI

def main(verbose=False):
    dsi = DSI(
        backend_name="NDP",
        keywords="energy",
        limit=20
    )
    
    # Get datasets and filter
    df = dsi.get_table("datasets", collection=True)
    
    print("\nFiltering datasets with more than 5 resources:")
    filtered = df.query("num_resources > 5")
    print(f"Found {len(filtered)} datasets")
    print(filtered[['title', 'num_resources']])
    
    if verbose:
        print("\n\nFiltering by organization:")
        if 'organization' in df.columns:
            orgs = df['organization'].value_counts()
            print(orgs.head())
            
            # Filter by top organization
            top_org = orgs.index[0]
            org_datasets = df[df['organization'] == top_org]
            print(f"\nDatasets from {top_org}: {len(org_datasets)}")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)