# # examples/ndp/ndp_developer/1.load_basic.py
# from dsi.core import Terminal

# def main(verbose=False):
#     terminal = Terminal()

#     # Load NDP backend (read-only)
#     terminal.load_module("backend", "NDP", "back-read", params={"keywords": "climate", "limit": 15})

#     backend = terminal.active_modules["back-read"][0]

#     if verbose:
#         artifacts = backend.summary()
#         print("Datasets loaded:")
#         # print(artifacts)
#         print(backend.display("datasets", num_rows=15, display_cols=["title", "organization", "num_resources"]))


# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--verbose", action="store_true")
#     args = parser.parse_args()
#     main(verbose=args.verbose)

# check_org_name.py
from dsi.core import Terminal

terminal = Terminal()

# Load with just "climate" keyword and no org filter
terminal.load_module(
    "backend", "NDP", "back-read",
    params={"keywords": "climate", "limit": 50}
)

backend = terminal.active_modules["back-read"][0]
data = backend.process_artifacts()

# Get all unique organizations from datasets
if "datasets" in data and "organization" in data["datasets"]:
    orgs = set(data["datasets"]["organization"])
    
    print("Organizations found with 'climate' keyword:")
    print("=" * 60)
    for org in sorted(orgs):
        if org:  # Skip None values
            print(f"  - {org}")
    
    # Check if California Landscape Metrics is there
    print("\n" + "=" * 60)
    if any("California" in str(org) for org in orgs if org):
        print("✓ Found California-related organization(s)")
    else:
        print("✗ No California organizations found with 'climate' keyword")

terminal.close()