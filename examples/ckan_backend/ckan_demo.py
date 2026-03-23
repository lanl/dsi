from dsi.core import Terminal

# Create terminal
terminal_ckan = Terminal()

# Load CKAN backend
# You can pass base_url or other config via kwargs
terminal_ckan.load_module(
    'backend',
    'CKAN',
    'back-read'
)

print("CKAN backend loaded successfully.")