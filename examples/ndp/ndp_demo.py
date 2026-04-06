# examples/ndp/ndp_demo.py

from dsi.core import Terminal
import pandas as pd


def demo_ndp(verbose=True):
    print("=== NDP Demo: Multi-Query + Validation + Inspection ===")

    # ----------------------------
    # 1) Initialize backend
    # ----------------------------
    t = Terminal()
    t.load_module("backend", "NDP", "back-read")
    backend = t.active_modules["back-read"][0]

    # ----------------------------
    # 2) First search
    # ----------------------------
    keyword = "wildfire"
    print(f"\n--- Searching NDP for '{keyword}' datasets ---")

    backend.query_artifacts(
        query=None,
        kwargs={"keywords": keyword, "limit": 20}
    )

    artifacts = backend.process_artifacts()
    df_ds = pd.DataFrame(artifacts.get("datasets", {}))
    df_res = pd.DataFrame(artifacts.get("resources", {}))

    print(f"Total datasets: {len(df_ds)}")
    print(f"Total resources: {len(df_res)}")

    # ----------------------------
    # ✅ TEST 1
    # ----------------------------
    assert len(df_ds) > 0, "No datasets returned"

    # ----------------------------
    # 3) Print organizations
    # ----------------------------
    print("\n--- Organizations ---")
    orgs = df_ds["organization"].dropna().unique()
    for o in orgs:
        print(f"- {o}")

    # ----------------------------
    # 4) Print name
    # ----------------------------
    print("\n--- Titles ---")
    titles = df_ds["title"].dropna().unique()
    for t in titles:
        print(f"- {t}")

    # ----------------------------
    # 5) Print tags
    # ----------------------------
    print("\n--- Tags ---")
    tag_set = set()
    for tgs in df_ds["tags"].dropna():
        for t in tgs.split(","):
            tag_set.add(t.strip())

    for t in sorted(tag_set):
        print(f"- {t}")

    # ----------------------------
    # 6) Query in-memory
    # ----------------------------
    print("\n--- Query: datasets with >5 resources ---")

    q1 = backend.query_in_memory(
        "`num_resources` > 5",
        {"table": "datasets"}
    )
    df_q1 = pd.DataFrame(q1)

    print(df_q1[["title", "organization", "num_resources"]])

    # ----------------------------
    # ✅ TEST 2
    # ----------------------------
    assert "num_resources" in df_q1.columns

    # ----------------------------
    # 7) Select dataset
    # ----------------------------
    print("\n--- Selecting dataset for deep inspection ---")

    if not df_ds.empty:
        # Prefer datasets with fewer resources
        filtered = df_ds[df_ds["num_resources"] < 10]

        if len(filtered) > 0:
            selected = filtered.iloc[0]
        else:
            selected = df_ds.iloc[0]

        ds_id = selected["id"]
        ds_title = selected["title"]

        print(f"Selected dataset:")
        print(f"  ID: {ds_id}")
        print(f"  Title: {ds_title}")

    else:
        print("No dataset available to select.")
        return

    # ----------------------------
    # 8) Re-query that dataset
    # ----------------------------
    print("\n--- Re-querying selected dataset ---")

    backend.query_artifacts(
        query=None,
        kwargs={"keywords": ds_id, "limit": 1}
    )

    artifacts2 = backend.process_artifacts()
    df_res2 = pd.DataFrame(artifacts2.get("resources", {}))

    print(f"Resources in selected dataset: {len(df_res2)}")

    # ----------------------------
    # 9) Validate URLs
    # ----------------------------
    print("\n--- Validating resource URLs ---")

    backend.validate_urls()
    validated = backend.process_artifacts().get("resources", {})
    df_valid = pd.DataFrame(validated)

    if not df_valid.empty:
        print(df_valid[["resource_name", "format", "url", "url_valid"]])

    # ----------------------------
    # ✅ TEST 3
    # ----------------------------
    assert "url_valid" in df_valid.columns

    # ----------------------------
    # 10) Query valid URLs
    # ----------------------------
    print("\n--- Query: valid URLs only ---")

    valid_only = backend.query_in_memory(
        "url_valid == True",
        {"table": "resources"}
    )
    df_valid_only = pd.DataFrame(valid_only)

    print(df_valid_only[["resource_name", "url"]].head())

    # ----------------------------
    # 11) Simulated download
    # ----------------------------
    print("\n--- Simulated download ---")

    if not df_valid_only.empty:
        sample_url = df_valid_only.iloc[0]["url"]
        print(f"Example downloadable resource:\n{sample_url}")

    # ----------------------------
    # 12) Final artifact snapshot
    # ----------------------------
    print("\n--- Final artifacts ---")

    final_artifacts = backend.get_artifacts()
    print("Datasets columns:", list(final_artifacts["datasets"].keys()))
    print("Resources columns:", list(final_artifacts["resources"].keys()))

    print("\n=== Demo complete ===")


if __name__ == "__main__":
    demo_ndp()