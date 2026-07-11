"""
Comprehensive test suite for NDP backend functionality.
Tests schema, find, search, summary, and other core operations.
"""

from dsi.dsi import DSI
import pandas as pd

def test_schema():
    """Test schema viewing with correct type mapping"""
    print("\n" + "="*60)
    print("TEST 1: SCHEMA OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "climate", "limit": 10})
    
    # Test full schema
    print("\n1a. Full schema:")
    schema = dsi.schema()
    print(schema)
    
    # Validate TEXT vs OBJECT distinction
    assert "TEXT" in schema, "Schema should contain TEXT type"
    assert "INTEGER" in schema, "Schema should contain INTEGER type"
    assert "REAL" in schema, "Schema should contain REAL type"
    assert "OBJECT" in schema, "Schema should contain OBJECT type for raw_dataset"
    
    # Test single table schema
    print("\n1b. Single table schema (datasets):")
    schema_single = dsi.schema("datasets")
    print(schema_single)
    assert "CREATE TABLE datasets" in schema_single
    
    print("\n1c. Single table schema (resources):")
    schema_resources = dsi.schema("resources")
    print(schema_resources)
    assert "CREATE TABLE resources" in schema_resources
    
    dsi.close()
    print("\nSCHEMA TEST: PASSED")


def test_find_operations():
    """Test find with various operators"""
    print("\n" + "="*60)
    print("TEST 2: FIND OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "fuel", "limit": 20})
    
    # Test less than
    print("\n2a. Find: num_resources < 5")
    results = dsi.find('num_resources < 5', collection=True)
    print(f"Found {len(results)} datasets with < 5 resources")
    if not results.empty:
        print(results[['title', 'num_resources']].head())
        assert all(results['num_resources'] < 5), "All results should have num_resources < 5"
    
    # Test greater than
    print("\n2b. Find: num_resources > 3")
    results = dsi.find('num_resources > 3', collection=True)
    print(f"Found {len(results)} datasets with > 3 resources")
    if not results.empty:
        assert all(results['num_resources'] > 3), "All results should have num_resources > 3"
    
    # Test range
    print("\n2c. Find: num_resources (3, 7)")
    results = dsi.find('num_resources (3, 7)', collection=True)
    print(f"Found {len(results)} datasets with 3-7 resources")
    if not results.empty:
        assert all((results['num_resources'] >= 3) & (results['num_resources'] <= 7))
    
    # Test equality
    print("\n2d. Find: num_resources == 4")
    results = dsi.find('num_resources == 4', collection=True)
    print(f"Found {len(results)} datasets with exactly 4 resources")
    if not results.empty:
        assert all(results['num_resources'] == 4)
    
    # Test contains (~~)
    print("\n2e. Find: title ~~ 'fire'")
    results = dsi.find("title ~~ 'fire'", collection=True)
    print(f"Found {len(results)} datasets with 'fire' in title")
    if not results.empty:
        print(results[['title', 'organization']].head())
        assert all('fire' in str(title).lower() for title in results['title'])
    
    dsi.close()
    print("\nFIND TEST: PASSED")


def test_search_operations():
    """Test search across all tables"""
    print("\n" + "="*60)
    print("TEST 3: SEARCH OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "earth science", "limit": 25})
    
    # Test string search
    print("\n3a. Search for 'NASA':")
    results = dsi.search("NASA", collection=True)
    if results:
        print(f"Found {len(results)} matching result(s)")
        for idx, result_df in enumerate(results, 1):
            print(f"\nMatch {idx}:")
            print(f"  Columns: {list(result_df.columns)}")
            print(f"  Rows: {len(result_df)}")
    
    # Test numeric search
    print("\n3b. Search for number 10:")
    results = dsi.search(10, collection=True)
    if results:
        print(f"Found {len(results)} matching result(s)")
    
    # Test format search
    print("\n3c. Search for 'CSV':")
    results = dsi.search("CSV", collection=True)
    if results:
        print(f"Found {len(results)} matching result(s)")
    
    dsi.close()
    print("\nSEARCH TEST: PASSED")


def test_summary_types():
    """Test summary shows correct type names"""
    print("\n" + "="*60)
    print("TEST 4: SUMMARY TYPE MAPPING")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "space", "limit": 10})
    
    # Test full summary
    print("\n4a. Full summary:")
    dsi.summary()
    
    # Test collection mode to validate types
    print("\n4b. Validating type mapping:")
    summary_dfs = dsi.summary(collection=True)
    
    for df in summary_dfs:
        print(f"\nTable summary types:")
        print(df[['column', 'type']].to_string(index=False))
        
        # Validate type names
        types = df['type'].unique()
        valid_types = {'TEXT', 'INTEGER', 'REAL', 'OBJECT', 'BOOLEAN', 'DATETIME'}
        
        assert all(t in valid_types for t in types), \
            f"Invalid types found: {set(types) - valid_types}"
    
    # Test single table summary
    print("\n4c. Datasets table summary:")
    datasets_summary = dsi.summary(table_name='datasets', collection=True)
    print(datasets_summary[['column', 'type', 'unique']].to_string(index=False))
    
    dsi.close()
    print("\nSUMMARY TEST: PASSED")


def test_list_operations():
    """Test list operations"""
    print("\n" + "="*60)
    print("TEST 5: LIST OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "climate", "limit": 10})
    
    # Test list print
    print("\n5a. List tables:")
    dsi.list()
    
    # Test list collection
    print("\n5b. List as collection:")
    table_list = dsi.list(collection=True)
    print(f"Tables: {table_list}")
    assert isinstance(table_list, list), "list(collection=True) should return list"
    assert 'datasets' in table_list, "Should have datasets table"
    
    dsi.close()
    print("\nLIST TEST: PASSED")


def test_get_table():
    """Test get_table operations"""
    print("\n" + "="*60)
    print("TEST 6: GET_TABLE OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "forest", "limit": 10})
    
    # Test DataFrame return
    print("\n6a. Get datasets as DataFrame:")
    datasets_df = dsi.get_table("datasets", collection=True)
    print(f"Shape: {datasets_df.shape}")
    print(f"Columns: {list(datasets_df.columns)}")
    assert isinstance(datasets_df, pd.DataFrame), "Should return DataFrame"
    
    # Test display (non-collection)
    print("\n6b. Get datasets (display mode):")
    dsi.get_table("datasets")
    
    dsi.close()
    print("\nGET_TABLE TEST: PASSED")


def test_display():
    """Test display operations"""
    print("\n" + "="*60)
    print("TEST 7: DISPLAY OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "earth", "limit": 10})
    
    # Test default display
    print("\n7a. Display datasets (default):")
    dsi.display('datasets', num_rows=3)
    
    # Test with specific columns
    print("\n7b. Display with selected columns:")
    dsi.display('datasets', num_rows=3, display_cols=['title', 'organization', 'num_resources'])
    
    # Test resources table
    print("\n7c. Display resources:")
    dsi.display('resources', num_rows=5)
    
    dsi.close()
    print("\nDISPLAY TEST: PASSED")


def test_num_datasets():
    """Test num_datasets vs num_tables"""
    print("\n" + "="*60)
    print("TEST 8: COUNTING OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "energy", "limit": 10})
    
    print("\n8a. Number of tables:")
    dsi.num_tables()
    
    print("\n8b. Number of datasets:")
    dsi.num_datasets()
    
    dsi.close()
    print("\nCOUNTING TEST: PASSED")


def test_complex_queries():
    """Test complex search and find combinations"""
    print("\n" + "="*60)
    print("TEST 9: COMPLEX OPERATIONS")
    print("="*60)
    
    dsi = DSI(backend_name="NDP", params={"keywords": "water quality", "limit": 20})
    
    # Combine find and get_table
    print("\n9a. Find datasets with many resources, then get full table:")
    filtered = dsi.find('num_resources >= 3', collection=True)
    print(f"Found {len(filtered)} datasets with >= 3 resources")
    
    if not filtered.empty:
        # Show sample
        print("\nSample results:")
        print(filtered[['title', 'num_resources', 'organization']].head())
    
    # Search and display
    print("\n9b. Search for 'CSV' and examine results:")
    search_results = dsi.search("CSV", collection=True)
    if search_results:
        print(f"Found {len(search_results)} table(s) with 'CSV'")
    
    dsi.close()
    print("\nCOMPLEX OPERATIONS TEST: PASSED")


def run_all_tests():
    """Run all test suites"""
    print("\n" + "="*60)
    print("RUNNING COMPLETE NDP TEST SUITE")
    print("="*60)
    
    try:
        test_schema()
        test_find_operations()
        test_search_operations()
        test_summary_types()
        test_list_operations()
        test_get_table()
        test_display()
        test_num_datasets()
        test_complex_queries()
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED!")
        print("="*60)
        
    except Exception as e:
        print("\n" + "="*60)
        print("TEST FAILED!")
        print("="*60)
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()