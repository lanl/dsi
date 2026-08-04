# examples/ndp/6.explore_metadata.py
"""
Explore available organizations, tags, and formats from search results.
Useful for discovering valid filter values before making targeted queries.
"""

from dsi.dsi import DSI

def main():
    # Query broadly to get a sample of datasets
    print("Loading sample datasets...")
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "climate", "limit": 50}
    )
    
    # Get the datasets and resources tables
    datasets_df = dsi.get_table("datasets", collection=True)
    resources_df = dsi.get_table("resources", collection=True)
    
    # Explore available organizations
    print("\n=== Available Organizations ===")
    print("Top 10 organizations by dataset count:")
    orgs = datasets_df['organization'].value_counts().head(10)
    print(orgs)
    
    # Explore available tags
    print("\n=== Available Tags ===")
    all_tags = []
    for tag_str in datasets_df['tags']:
        if tag_str:
            all_tags.extend(tag_str.split(','))
    
    unique_tags = sorted(set(tag.strip() for tag in all_tags))
    print(f"Found {len(unique_tags)} unique tags")
    print("First 20 tags:", unique_tags[:20])
    
    # Explore available formats
    print("\n=== Available Resource Formats ===")
    print("Formats by resource count:")
    formats = resources_df['format'].value_counts()
    print(formats)
    
    dsi.close()

if __name__ == "__main__":
    main()