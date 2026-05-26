from dsi.core import Terminal

def demo_earthquake_orgs():
    print("=== Earthquake Datasets - All Organizations ===\n")
    
    terminal = Terminal()
    
    # Load earthquake datasets
    print("Searching NDP for earthquake datasets...\n")
    terminal.load_module(
        "backend",
        "NDP",
        "back-read",
        # params={"keyword": "salton", "limit": 100}

        params={"organization": "ucr-earth-and-planetary-sciences", "limit": 100}
    )
    
    # Display all datasets with key columns
    print("All Earthquake Datasets:")
    print("-" * 70)
    terminal.display(
        "datasets",
        num_rows=100,  # Show all
        display_cols=["title", "organization", "num_resources"]
    )
    
    # Get unique organizations
    backend = terminal.active_modules["back-read"][0]
    artifacts = backend.process_artifacts()
    
    if "datasets" in artifacts:
        import pandas as pd
        df = pd.DataFrame(artifacts["datasets"])
        
        if "organization" in df.columns:
            org_counts = df["organization"].value_counts()
            
            print("\n\nOrganization Summary:")
            print("-" * 70)
            for org, count in org_counts.items():
                print(f"{org}: {count} datasets")
            
            print(f"\nTotal organizations: {len(org_counts)}")
            print(f"Total datasets: {len(df)}")
    
    terminal.close()


if __name__ == "__main__":
    demo_earthquake_orgs()