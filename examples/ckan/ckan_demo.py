from dsi.core import Terminal
import pandas as pd

def demo_ckan_climate_change():
    print("=== CKAN Climate Change Demo ===")
    
    # ----------------------------
    # 1) Initialize backend
    # ----------------------------
    t = Terminal()
    t.load_module("backend", "CKAN", "back-read")
    backend = t.active_modules["back-read"][0]
    
    # ----------------------------
    # 2) First search: climate change
    # ----------------------------
    keyword = "climate change"
    print(f"\n--- Searching CKAN for '{keyword}' datasets ---")
    backend.ingest_artifacts(artifacts=None, kwargs={"keywords": keyword, "limit": 50})
    
    artifacts = backend.process_artifacts()
    datasets = artifacts.get("datasets", {})
    
    total_found = len(datasets.get("id", []))
    print(f"Total '{keyword}' datasets found: {total_found}")

    # ----------------------------
    # 3) Top 20 datasets
    # ----------------------------
    print(f"\n--- Top 20 '{keyword}' dataset summaries ---")
    df_ds = pd.DataFrame(datasets).sort_values(by="num_resources", ascending=False)
    top20 = df_ds.head(20)

    for idx, row in top20.iterrows():
        print(
            f"{idx+1:2}. Title: {row['title']} | Org: {row['organization']} | Resources: {row['num_resources']}"
        )

    # ----------------------------
    # 4) Filter: <10 resources
    # ----------------------------
    print(f"\n--- Filtering '{keyword}' datasets with < 10 resources ---")
    filtered = df_ds[df_ds["num_resources"] < 10]
    print(f"Count (<10 resources): {len(filtered)}")

    if len(filtered) == 0:
        print("No datasets with <10 resources found.")
        return

    # ----------------------------
    # 5) Select the first dataset
    # ----------------------------
    first_ds = filtered.iloc[2]
    ds_id = first_ds["id"]
    ds_title = first_ds["title"]
    
    print(f"\nSelected dataset for deeper inspection:\n  ID: {ds_id}\n  Title: {ds_title}")

    # ----------------------------
    # 6) Re-ingest just this dataset
    # ----------------------------
    print("\n--- Re-ingesting single dataset by ID ---")
    backend.ingest_artifacts(
        artifacts=None,
        kwargs={"keywords": ds_id, "limit": 1}
    )

    artifacts = backend.process_artifacts()
    resources = artifacts.get("resources", {})

    # ----------------------------
    # 7) Validate resource URLs
    # ----------------------------
    print("\n--- Validating resource URLs ---")
    backend.validate_urls()
    resources_validated = backend.process_artifacts().get("resources", {})

    # Print details
    df_res = pd.DataFrame(resources_validated)
    print("\nResources with validity:")
    print(df_res[["resource_name", "format", "url", "url_valid"]])

    print("\n=== Demo complete ===")

if __name__ == "__main__":
    demo_ckan_climate_change()