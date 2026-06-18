# examples/ndp/ndp_user/author_search.py
from dsi.dsi import DSI

def main(verbose=False):
    """
    Demonstrates a two-step search workflow:
    1. Query organization to get all datasets
    2. Filter results by specific author
    
    This is useful when you want to find a specific author's work
    within a known organization.
    """
    
    # Step 1: Query organization to get all datasets
    print("\n=== Step 1: Loading all datasets from Oceans11 - LANL ===")
    dsi = DSI(
        backend_name="NDP",
        organization="BurnPro3D",
        limit=50  # Get more results to find author
    )
    
    datasets_df = dsi.get_table("datasets", collection=True)
    
    if datasets_df.empty:
        print("No datasets found for this organization")
        dsi.close()
        return
    
    print(f"Found {len(datasets_df)} total datasets from Oceans11 - LANL")
    
    # Step 2: Filter by specific author
    print("\n=== Step 2: Filtering for author 'Leticia Lee' ===")
    
    # Use pandas to filter by author
    author_name = "Lee"  # Can use partial name
    author_datasets = datasets_df[
        datasets_df['author'].str.contains(author_name, case=False, na=False)
    ]
    
    if author_datasets.empty:
        print(f"No datasets found for author containing '{author_name}'")
        
        if verbose:
            print("\n--- Available authors in this organization ---")
            unique_authors = datasets_df['author'].dropna().unique()
            for author in sorted(unique_authors):
                print(f"  - {author}")
    else:
        print(f"Found {len(author_datasets)} dataset(s) by authors matching '{author_name}'")
        
        # Display author's datasets
        print("\n--- Dataset Details ---")
        for idx, row in author_datasets.iterrows():
            print(f"\n{idx+1}. {row['title']}")
            print(f"   Author: {row['author']}")
            print(f"   Organization: {row['organization']}")
            print(f"   Created: {row['created']}")
            print(f"   Resources: {row['num_resources']}")
            
            if row['tags']:
                print(f"   Tags: {row['tags']}")
        
        if verbose:
            # Show resource files for author's datasets
            print("\n--- Resource Files (First Dataset) ---")
            table_list = dsi.list(collection=True)
            resource_tables = [t for t in table_list if t != 'datasets']
            
            # Find resource table for first author dataset
            first_dataset_title = author_datasets.iloc[0]['title']
            
            if first_dataset_title in resource_tables:
                resource_df = dsi.get_table(first_dataset_title, collection=True)
                
                print(f"\nResources for '{first_dataset_title}':")
                for idx, row in resource_df.iterrows():
                    print(f"\n  Resource {idx+1}:")
                    print(f"    Name: {row['resource_name']}")
                    print(f"    Format: {row['format']}")
                    if row.get('issue_date'):
                        print(f"    Date: {row['issue_date']}")
                    if row.get('size'):
                        print(f"    Size: {row['size']} bytes")
                    print(f"    URL: {row['url']}")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(verbose=args.verbose)