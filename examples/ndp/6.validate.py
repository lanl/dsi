# examples/ndp/6.validate.py

from dsi.core import Terminal

# Initialize terminal and load NDP backend
t = Terminal()
t.load_module("backend", "NDP", "back-read")
backend = t.active_modules["back-read"][0]

# Fetch artifacts
backend.query_artifacts(
    query=None,
    kwargs={"keywords": "wildfire", "limit": 8}  # adjust limit if needed
)

artifacts = backend.process_artifacts()
print("Datasets loaded:", artifacts["datasets"].keys())
print("Resources loaded:", artifacts["resources"].keys())

# Validate URLs
backend.validate_urls()

# Get resources table
resources = backend.process_artifacts().get("resources", {})

if not resources or not resources.get("url"):
    print("No resources found. Try increasing 'limit' or check your query.")
else:
    urls = resources["url"]
    url_valid = resources.get("url_valid", [])
    for i, (u, v) in enumerate(zip(urls[:5], url_valid[:5])):
        print(f"{i+1}. {u} -> {'Valid' if v else 'Invalid'}")