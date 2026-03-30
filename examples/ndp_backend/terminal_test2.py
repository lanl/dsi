# test_terminal_with_ckan.py
from dsi.core import Terminal
from DSI.dsi.backends.ckan_v1 import CKAN

# Create Terminal directly
terminal = Terminal()

# Load CKAN backend
ckan = CKAN(
    base_url='https://nationaldataplatform.org/catalog',
    verify_ssl=False
)

ckan.load_metadata(keywords='wildfire', formats=['CSV'], limit=10)
data = ckan.process_artifacts()

# Register CKAN with Terminal
terminal.loaded_backends.append(ckan)

# Use Terminal methods directly
print("\n--- Terminal.find_table() ---")
table_matches = terminal.find_table("datasets")
if table_matches:
    for val in table_matches:
        print(f"Table: {val.t_name}, Type: {val.type}")

print("\n--- Terminal.find_column() ---")
col_matches = terminal.find_column("title")
if col_matches:
    for val in col_matches:
        print(f"Table: {val.t_name}, Column: {val.c_name}")

print("\n--- Terminal.find_cell() ---")
cell_matches = terminal.find_cell("CSV")
if cell_matches:
    for val in cell_matches[:5]:  # First 5 matches
        print(f"Table: {val.t_name}, Column: {val.c_name}, Value: {val.value}")

print("\n--- Terminal.find_cell() with row=True ---")
row_matches = terminal.find_cell("wildfire", row=True)
if row_matches:
    for val in row_matches[:3]:  # First 3 rows
        print(f"Table: {val.t_name}, Row: {val.row_num}")
        print(f"  Columns: {val.c_name}")
        print(f"  Values: {val.value}\n")

print("\n--- Terminal.find() - All matches ---")
all_matches = terminal.find("climate")
if all_matches:
    for val in all_matches[:5]:
        print(f"Type: {val.type}, Table: {val.t_name}, Column: {val.c_name}")
