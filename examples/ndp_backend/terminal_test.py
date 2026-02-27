# test_dsi_with_ckan.py
from dsi.dsi import DSI
from dsi.backends.ckan import CKAN

# Initialize
dsi = DSI()

# Create CKAN backend
ckan = CKAN(
    base_url='https://nationaldataplatform.org/catalog',
    verify_ssl=False
)

# Load data into CKAN
print("Loading metadata...")
ckan.load_metadata(
    keywords='wildfire',
    formats=['CSV'],
    limit=10
)

# Get data from CKAN and ingest into DSI's terminal
print("\nIngesting CKAN data into DSI...")
ckan_data = ckan.process_artifacts()  # Get OrderedDict from CKAN

# Check if Terminal has an ingest method
if hasattr(dsi, 'terminal'):
    # Add CKAN to Terminal's backends
    dsi.terminal.loaded_backends.append(ckan)
    print("✓ CKAN registered with Terminal")
    
    # Try Terminal's list
    print("\nTerminal.list():")
    try:
        result = dsi.terminal.list()
        print(result)
    except Exception as e:
        print(f"Error: {e}")

# Or try accessing data directly
print("\nAccessing CKAN data directly:")
print(f"  Tables: {list(ckan_data.keys())}")
for table_name in ckan_data.keys():
    if ckan_data[table_name]:
        num_rows = len(next(iter(ckan_data[table_name].values())))
        num_cols = len(ckan_data[table_name])
        print(f"  {table_name}: {num_rows} rows, {num_cols} columns")
