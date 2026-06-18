# examples/ndp/ndp_user/5.org_tag_filter.py
from dsi.dsi import DSI

def main(verbose=False):
    """
    Filter by organization and tags across multiple queries.
    """
    dsi = DSI(
        backend_name="NDP",
        params=[
            {
                "organization": "WFSI",
                # "keyword": "burn",
                "tags": ["duff"],
                "limit": 20
            },
            {
                "organization": "Planscape",
                # "tags": ["nasa"],
                "limit": 10
            }
        ]
    )
    
    print("\n=== Organization + Tag Filtering ===")
    dsi.list()
    
    if verbose:
        # Get all datasets and group by organization
        datasets_df = dsi.get_table("datasets", collection=True)
        
        print("\nDatasets by Organization:")
        for org in datasets_df['organization'].unique():
            count = len(datasets_df[datasets_df['organization'] == org])
            print(f"  {org}: {count} datasets")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)