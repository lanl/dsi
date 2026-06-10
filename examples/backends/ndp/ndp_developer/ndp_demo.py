# examples/ndp/ndp_developer/ndp_demo.py

from dsi.core import Terminal
import pandas as pd


def demo_ndp(verbose=True):
    print("=== NDP Demo: Multi-Query + Validation + Inspection ===")

    # ----------------------------
    # 1) Initialize backend
    # ----------------------------
    t = Terminal()
    t.load_module(
        "backend",
        "NDP",
        "back-read",
        params={"keywords": "climate", "limit": 20}
    )
    backend = t.active_modules["back-read"][0]

    # ----------------------------
    # 2) First search - Process artifacts
    # ----------------------------
    keyword = "climate"
    print(f"\n--- Searching NDP for '{keyword}' datasets ---")

    artifacts = backend.process_artifacts()
    df_ds = pd.DataFrame(artifacts.get("datasets", {}))
    
    # Count total resources across all resource tables
    total_resources = sum(
        len(table.get('url', []))
        for name, table in artifacts.items()
        if name != "datasets"
    )

    print(f"Total datasets: {len(df_ds)}")
    print(f"Total resources: {total_resources}")
    print(f"Resource tables: {len(backend._resource_tables)}")

    assert len(df_ds) > 0, "No datasets returned"

    # ----------------------------
    # 3) Print organizations
    # ----------------------------
    print("\n--- Organizations ---")
    if "organization" in df_ds.columns:
        orgs = df_ds["organization"].dropna().unique()
        for o in orgs[:10]:  # Show first 10
            print(f"- {o}")

    # ----------------------------
    # 4) Print titles
    # ----------------------------
    print("\n--- Titles (first 10) ---")
    if "title" in df_ds.columns:
        titles = df_ds["title"].dropna().unique()
        for t in titles[:10]:
            print(f"- {t}")

    # ----------------------------
    # 5) Print tags
    # ----------------------------
    print("\n--- Tags (first 20) ---")
    if "tags" in df_ds.columns:
        tag_set = set()
        for tgs in df_ds["tags"].dropna():
            if isinstance(tgs, str):
                for t in tgs.split(","):
                    tag_set.add(t.strip())

        for t in sorted(list(tag_set)[:20]):
            print(f"- {t}")

    # ----------------------------
    # 6) Query in-memory
    # ----------------------------
    print("\n--- Query: datasets with >5 resources ---")

    q1_result = backend.query_artifacts(
        query="`num_resources` > 5",
        dict_return=True
    )
    
    if "datasets" in q1_result:
        df_q1 = pd.DataFrame(q1_result["datasets"])
        print(f"\nFound {len(df_q1)} datasets with >5 resources")
        
        if not df_q1.empty and all(col in df_q1.columns for col in ["title", "organization", "num_resources"]):
            print("\nFirst 5 results:")
            for idx in range(min(5, len(df_q1))):
                row = df_q1.iloc[idx]
                title_display = row['title'][:50] + '...' if len(str(row['title'])) > 50 else row['title']
                print(f"  {idx+1}. {row['num_resources']} resources - {title_display} ({row['organization']})")

    assert "datasets" in q1_result, "Query should return datasets table"

    # ----------------------------
    # 7) Select dataset for inspection
    # ----------------------------
    print("\n--- Selecting dataset for deep inspection ---")

    if not df_ds.empty:
        # Prefer datasets with moderate number of resources (2-10)
        filtered = df_ds[(df_ds["num_resources"] >= 2) & (df_ds["num_resources"] <= 10)]

        if len(filtered) > 0:
            selected = filtered.iloc[0]
        else:
            selected = df_ds.iloc[0]

        ds_id = selected["id"]
        ds_title = selected["title"]
        ds_num_resources = selected["num_resources"]

        print("Selected dataset:")
        print(f"  ID: {ds_id}")
        print(f"  Title: {ds_title}")
        print(f"  Resources: {ds_num_resources}")
    else:
        print("No dataset available to select.")
        return

    # ----------------------------
    # 8) Inspect selected dataset's resources
    # ----------------------------
    print("\n--- Inspecting selected dataset's resources ---")

    # The resource table name is the dataset title
    if ds_title in artifacts:
        df_res = pd.DataFrame(artifacts[ds_title])
        print(f"\nResources in '{ds_title}': {len(df_res)}")
        
        if not df_res.empty and all(col in df_res.columns for col in ["resource_name", "format", "url"]):
            print("\nResource details (first 3):")
            for idx in range(min(3, len(df_res))):
                row = df_res.iloc[idx]
                print(f"  {idx+1}. {row['resource_name']} ({row['format']})")
                print(f"     URL: {row['url'][:70]}...")
    else:
        print(f"No resources found for dataset '{ds_title}'")

    # ----------------------------
    # 9) Validate URLs
    # ----------------------------
    print("\n--- Validating resource URLs ---")

    backend.validate_urls()
    validated = backend.process_artifacts()

    # Show validation results for selected dataset
    if ds_title in validated:
        df_valid = pd.DataFrame(validated[ds_title])
        
        if "url_valid" in df_valid.columns:
            valid_count = df_valid["url_valid"].sum()
            total_count = len(df_valid)
            print(f"\nValidation results for '{ds_title}':")
            print(f"  Valid URLs: {valid_count}/{total_count}")
            
            if not df_valid.empty:
                print("\nFirst 3 resources:")
                for idx in range(min(3, len(df_valid))):
                    row = df_valid.iloc[idx]
                    status = "✓ Valid" if row.get("url_valid", False) else "✗ Invalid"
                    print(f"  {status} - {row.get('resource_name', 'N/A')} ({row.get('format', 'N/A')})")

    assert ds_title in validated, "Selected dataset should exist in validated artifacts"
    if ds_title in validated:
        assert "url_valid" in validated[ds_title], "url_valid column should exist"

    # ----------------------------
    # 10) Query valid URLs only
    # ----------------------------
    print("\n--- Query: valid URLs only ---")

    try:
        valid_only_result = backend.query_artifacts(
            query="url_valid == True",
            dict_return=True
        )
        
        valid_tables_count = len([t for t in valid_only_result.keys() if t != "datasets"])
        print(f"Found {valid_tables_count} resource tables with valid URLs")
        
        # Show first valid resource from first table
        if ds_title in valid_only_result:
            df_valid_only = pd.DataFrame(valid_only_result[ds_title])
            print(f"\nValid URLs in '{ds_title}': {len(df_valid_only)}")
            
            if not df_valid_only.empty and "resource_name" in df_valid_only.columns:
                print("\nFirst 3 valid resources:")
                for idx in range(min(3, len(df_valid_only))):
                    row = df_valid_only.iloc[idx]
                    print(f"  {idx+1}. {row['resource_name']}")
                    if "url" in row:
                        print(f"     {row['url'][:70]}...")
    except ValueError as e:
        print(f"Note: {e}")

    # ----------------------------
    # 11) Example downloadable resource
    # ----------------------------
    print("\n--- Example downloadable resource ---")

    if ds_title in validated:
        table_data = validated[ds_title]
        if "url_valid" in table_data and "url" in table_data:
            valid_indices = [i for i, v in enumerate(table_data["url_valid"]) if v]
            if valid_indices:
                sample_idx = valid_indices[0]
                sample_url = table_data["url"][sample_idx]
                sample_name = table_data.get("resource_name", ["Unknown"])[sample_idx]
                print(f"Example valid resource: {sample_name}")
                print(f"URL: {sample_url}")

    # ----------------------------
    # 12) Find operations
    # ----------------------------
    print("\n--- Find operations ---")

    # Find tables
    tables_found = backend.find_table("datasets")
    print(f"\nTables matching 'datasets': {len(tables_found)}")
    
    # Find columns
    columns_found = backend.find_column("title")
    print(f"Columns matching 'title': {len(columns_found)}")
    
    # Find cells
    cells_found = backend.find_cell(ds_title[:10] if len(ds_title) > 10 else ds_title)
    print(f"Cells containing '{ds_title[:10]}': {len(cells_found)}")

    # ----------------------------
    # 13) Final artifact snapshot
    # ----------------------------
    print("\n--- Final artifacts ---")

    final_artifacts = backend.process_artifacts()
    print(f"Total tables: {len(final_artifacts)}")
    
    datasets_count = len(final_artifacts.get('datasets', {}).get('id', []))
    print(f"  - datasets: {datasets_count} rows")
    
    resource_tables = [k for k in final_artifacts.keys() if k != "datasets"]
    print(f"  - resource tables: {len(resource_tables)}")

    print("\n=== Demo complete ===")


if __name__ == "__main__":
    demo_ndp()