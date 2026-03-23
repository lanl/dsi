from dsi.core import Terminal

t = Terminal()

# Load CKAN as a read-only backend
t.load_module(
    "backend",
    "CKAN",
    "back-read"
)

print("Backends loaded:")
print(t.module_collection["backend"].keys())