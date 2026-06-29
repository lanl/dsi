# examples/ndp/ndp_user/test_compare_schema.py

from dsi.dsi import DSI
import os

def print_separator(title="", char="=", width=80):
    """Print a formatted separator line"""
    if title:
        print(f"\n{char * width}")
        print(title)
        print(char * width)
    else:
        print(char * width)

def print_section(title):
    """Print a section header"""
    print(f"\n{'-' * 80}")
    print(title)
    print('-' * 80)

def test_sqlite():
    """Test SQLite backend with Cloverleaf3D data"""
    print_separator("SQLITE BACKEND (Cloverleaf3D) OUTPUTS", "=")
    
    # Check if schema and data files exist
    schema_path = "../../clover3d/schema.json"
    data_path = "../../clover3d/"
    
    if not os.path.exists(schema_path):
        print(f"ERROR: Schema file not found: {schema_path}")
        return
    if not os.path.exists(data_path):
        print(f"ERROR: Data directory not found: {data_path}")
        return
    
    schema_dsi = DSI("schema_data.db")
    print(f"Created an instance of DSI with the Sqlite backend: schema_data.db")
    
    # Load schema and data
    schema_dsi.schema(schema_path)
    schema_dsi.read(data_path, 'Cloverleaf')
    print(f"Loaded Cloverleaf3D data with schema")
    
    # # 1. list() - Default (prints)
    # print_section("1. list() - Default (prints)")
    # schema_dsi.list()
    
    # # 2. list(collection=True) - Returns list
    # print_section("2. list(collection=True) - Returns list")
    # result = schema_dsi.list(collection=True)
    # print(f"Type: {type(result)}")
    # print(f"Contents: {result}")
    
    # # 3. summary() - Default (prints)
    # print_section("3. summary() - Default (prints)")
    # schema_dsi.summary()
    
    # # 4. summary(table_name) - Single table
    # print_section("4. summary(table_name='simulation') - Single table")
    # schema_dsi.summary(table_name='simulation')
    
    # # 5. summary(collection=True) - Returns DataFrame
    # print_section("5. summary(collection=True) - Returns DataFrame/List")
    # result = schema_dsi.summary(collection=True)
    # print(f"Type: {type(result)}")
    # if isinstance(result, list):
    #     print(f"Length: {len(result)}")
    #     if len(result) > 0:
    #         print(f"First element type: {type(result[0])}")
    
    # 6. display() - Default
    print_section("6. display('simulation') - Default")
    schema_dsi.display('simulation')
    
    # 7. display() with specific columns
    print_section("7. display('input', [...]) - Specific columns")
    schema_dsi.display("input", ["sim_id", "state1_density", "state2_density", "initial_timestep", "end_step"])
    
    # 8. display() with num_rows
    print_section("8. display('output', num_rows=5)")
    schema_dsi.display("output", num_rows=5)
    
    print("\nClosing SQLite instance")
    schema_dsi.close()

def test_ndp():
    """Test NDP backend"""
    print_separator("NDP BACKEND OUTPUTS", "=")
    
    ndp_dsi = DSI(backend_name='ndp', params={"keyword": "water", "limit": 3})
    print("Created an instance of DSI with NDP backend")
    
    # # 1. list() - Default (prints)
    # print_section("1. list() - Default (prints)")
    # ndp_dsi.list()
    
    # # 2. list(collection=True) - Returns list
    # print_section("2. list(collection=True) - Returns list")
    # result = ndp_dsi.list(collection=True)
    # print(f"Type: {type(result)}")
    # print(f"Contents: {result}")
    
    # # 3. summary() - Default (prints)
    # print_section("3. summary() - Default (prints)")
    # ndp_dsi.summary()
    
    # # 4. summary(table_name) - Single table
    # print_section("4. summary(table_name='datasets') - Single table")
    # ndp_dsi.summary(table_name='datasets')
    
    # # 5. summary(collection=True) - Returns DataFrame
    # print_section("5. summary(collection=True) - Returns DataFrame/List")
    # result = ndp_dsi.summary(collection=True)
    # print(f"Type: {type(result)}")
    # if isinstance(result, list):
    #     print(f"Length: {len(result)}")
    #     if len(result) > 0:
    #         print(f"First element type: {type(result[0])}")
    
    # 6. display() - Default
    print_section("6. display('datasets') - Default")
    ndp_dsi.display('datasets')
    
    # 7. display() with specific columns
    print_section("7. display('datasets', [...]) - Specific columns")
    ndp_dsi.display('datasets', ['id', 'name', 'title', 'organization', 'num_resources'])
    
    # 8. display() with num_rows
    print_section("8. display('resources', num_rows=5)")
    ndp_dsi.display('resources', num_rows=5)
    
    print("\nClosing NDP instance")
    ndp_dsi.close()

def main():
    """Run comparison tests"""
    print("\n" + "=" * 80)
    print("DSI BACKEND COMPARISON TEST")
    print("SQLite (Cloverleaf3D) vs NDP (keyword search)")
    print("=" * 80)
    
    # Test SQLite first
    try:
        test_sqlite()
    except Exception as e:
        print(f"\n❌ ERROR in SQLite test: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" * 3)  # Add space between backends
    
    # Test NDP
    try:
        test_ndp()
    except Exception as e:
        print(f"\n❌ ERROR in NDP test: {e}")
        import traceback
        traceback.print_exc()
    
    print_separator("COMPARISON TEST COMPLETE")

if __name__ == "__main__":
    main()