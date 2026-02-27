
# test_all.py - CORRECTED VERSION
"""
DSI-CKAN Backend Integration Tests
Tests using DSI methods with CKAN backend
"""
from dsi.dsi import DSI
from dsi.backends.ckan import CKAN
import pandas as pd

# ============================================================================
#                    TEST 1: Basic CKAN Backend Loading
# ============================================================================
def test_basic_ckan_backend():
    """Test basic CKAN backend setup and data loading"""
    print("\n" + "="*70)
    print("TEST 1: Basic CKAN Backend Setup")
    print("="*70)
    
    dsi = DSI()
    
    # Create and configure CKAN backend
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    # Load metadata
    print("\n--- Loading CKAN metadata ---")
    ckan.load_metadata(
        keywords='climate',
        formats=['CSV'],
        limit=10
    )
    
    # Process data
    data = ckan.process_artifacts()
    
    # Load into DSI using read() with collection
    print("\n--- Loading into DSI ---")
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    # Verify data loaded
    print(f"\n✓ Tables loaded: {list(data.keys())}")
    for table_name in data.keys():
        if data[table_name]:
            num_rows = len(next(iter(data[table_name].values())))
            num_cols = len(data[table_name])
            print(f"  • {table_name}: {num_rows} rows × {num_cols} columns")


# ============================================================================
#                    TEST 2: DSI Query on CKAN Data
# ============================================================================
def test_dsi_query_ckan():
    """Test DSI query() method on CKAN-loaded data"""
    print("\n" + "="*70)
    print("TEST 2: DSI Query on CKAN Data")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    # Load data
    ckan.load_metadata(keywords='wildfire', formats=['CSV'], limit=10)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    # Get table names
    table_names = list(data.keys())
    print(f"\nAvailable tables: {table_names}")
    
    # Query each table
    for table_name in table_names[:2]:  # Test first 2 tables
        print(f"\n--- Querying table: {table_name} ---")
        
        try:
            # Query all data
            dsi.query(f"SELECT * FROM {table_name} LIMIT 5")
            
            # Count query
            dsi.query(f"SELECT COUNT(*) as total_records FROM {table_name}")
            
        except Exception as e:
            print(f"Query error: {e}")


# ============================================================================
#                    TEST 3: DSI get_table() on CKAN Data
# ============================================================================
def test_dsi_get_table():
    """Test DSI get_table() method"""
    print("\n" + "="*70)
    print("TEST 3: DSI get_table() Method")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV'], limit=5)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    table_name = list(data.keys())[0]
    
    # Get table without collection
    print(f"\n--- get_table() print mode ---")
    dsi.get_table(table_name)
    
    # Get table as collection
    print(f"\n--- get_table() collection mode ---")
    df = dsi.get_table(table_name, collection=True)
    print(f"Returned DataFrame: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Get table for update
    print(f"\n--- get_table() with update flag ---")
    df_update = dsi.get_table(table_name, collection=True, update=True)
    print(f"Update DataFrame columns: {list(df_update.columns)}")
    print(f"Has dsi_table_name column: {'dsi_table_name' in df_update.columns}")


# ============================================================================
#                    TEST 4: DSI find() on CKAN Data
# ============================================================================
def test_dsi_find():
    """Test DSI find() method on CKAN data"""
    print("\n" + "="*70)
    print("TEST 4: DSI find() Method")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV', 'JSON'], limit=15)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    # Test various find operations
    test_queries = [
        "num_resources > 2",
        "num_resources > 1"
    ]
    
    for query in test_queries:
        print(f"\n--- find('{query}') ---")
        try:
            dsi.find(query)
        except Exception as e:
            print(f"Find error: {e}")
    
    # Get find results as collection
    print(f"\n--- find() as collection ---")
    try:
        df = dsi.find("num_resources > 1", collection=True)
        if df is not None:
            print(f"Found {len(df)} matching rows")
    except Exception as e:
        print(f"Find collection error: {e}")


# ============================================================================
#                    TEST 5: DSI search() on CKAN Data
# ============================================================================
def test_dsi_search():
    """Test DSI search() method"""
    print("\n" + "="*70)
    print("TEST 5: DSI search() Method")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='wildfire', formats=['CSV'], limit=10)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    # Search for different values
    search_terms = ['CSV', 'wildfire', 'climate']
    
    for term in search_terms:
        print(f"\n--- Searching for: '{term}' ---")
        try:
            dsi.search(term)
        except Exception as e:
            print(f"Search error: {e}")
    
    # Search as collection
    print(f"\n--- search() as collection ---")
    try:
        results = dsi.search('CSV', collection=True)
        if results:
            print(f"Found matches in {len(results)} locations")
            for i, df in enumerate(results):
                print(f"  Result {i+1}: {df.shape}")
    except Exception as e:
        print(f"Search collection error: {e}")


# ============================================================================
#                    TEST 6: Multiple CKAN Data Loads
# ============================================================================
def test_multiple_ckan_loads():
    """Test loading multiple CKAN queries into same DSI instance"""
    print("\n" + "="*70)
    print("TEST 6: Multiple CKAN Data Loads")
    print("="*70)
    
    dsi = DSI()
    
    # Load 1: Climate data
    print("\n--- Load 1: Climate Data ---")
    ckan1 = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    ckan1.load_metadata(keywords='climate', formats=['CSV'], limit=5)
    data1 = ckan1.process_artifacts()
    
    for table_name, table_data in data1.items():
        if table_data:
            # Rename table to avoid conflicts
            new_table_name = f"climate_{table_name}"
            dsi.read(table_data, reader_name='collection', table_name=new_table_name)
    print(f"Loaded tables: {list(data1.keys())}")
    
    # Load 2: Wildfire data
    print("\n--- Load 2: Wildfire Data ---")
    ckan2 = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    ckan2.load_metadata(keywords='wildfire', formats=['CSV'], limit=5)
    data2 = ckan2.process_artifacts()
    
    for table_name, table_data in data2.items():
        if table_data:
            # Rename table to avoid conflicts
            new_table_name = f"wildfire_{table_name}"
            dsi.read(table_data, reader_name='collection', table_name=new_table_name)
    print(f"Loaded tables: {list(data2.keys())}")
    
    # Query combined data
    print("\n--- Querying combined data ---")
    try:
        dsi.query("SELECT * FROM climate_datasets LIMIT 3")
        dsi.query("SELECT * FROM wildfire_datasets LIMIT 3")
    except Exception as e:
        print(f"Query error: {e}")


# ============================================================================
#                    TEST 7: CKAN Data Export with DSI write()
# ============================================================================
def test_dsi_write_ckan():
    """Test DSI write() method with CKAN data"""
    print("\n" + "="*70)
    print("TEST 7: DSI write() Method")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV'], limit=10)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    table_name = list(data.keys())[0]
    
    # Test CSV export
    print(f"\n--- Exporting {table_name} to CSV ---")
    try:
        dsi.write(f'ckan_{table_name}.csv', 'Csv', table_name=table_name)
        print("✓ CSV export successful")
    except Exception as e:
        print(f"CSV export error: {e}")
    
    # Test Parquet export
    print(f"\n--- Exporting {table_name} to Parquet ---")
    try:
        dsi.write(f'ckan_{table_name}.pq', 'Parquet', table_name=table_name)
        print("✓ Parquet export successful")
    except Exception as e:
        print(f"Parquet export error: {e}")


# ============================================================================
#                    TEST 8: CKAN Data Update Workflow
# ============================================================================
def test_dsi_update_workflow():
    """Test complete update workflow with CKAN data"""
    print("\n" + "="*70)
    print("TEST 8: Update Workflow")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV'], limit=5)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    table_name = list(data.keys())[0]
    
    # Step 1: Get data for update
    print("\n--- Step 1: Get data for update ---")
    df = dsi.get_table(table_name, collection=True, update=True)
    print(f"Original shape: {df.shape}")
    print(f"Columns: {list(df.columns)[:5]}...")
    
    # Step 2: Modify data (add a new column)
    print("\n--- Step 2: Modify data ---")
    df['test_column'] = 'test_value'
    print(f"Modified shape: {df.shape}")
    print(f"New columns: {list(df.columns)}")
    
    # Step 3: Update (with backup)
    print("\n--- Step 3: Update with backup ---")
    try:
        dsi.update(df, backup=True)
        print("✓ Update successful")
    except Exception as e:
        print(f"Update error: {e}")
    
    # Step 4: Verify update
    print("\n--- Step 4: Verify update ---")
    df_verify = dsi.get_table(table_name, collection=True)
    print(f"Verified shape: {df_verify.shape}")
    print(f"Has test_column: {'test_column' in df_verify.columns}")


# ============================================================================
#                    TEST 9: CKAN Organization Filter
# ============================================================================
def test_organization_filter():
    """Test filtering by organization"""
    print("\n" + "="*70)
    print("TEST 9: Organization Filter")
    print("="*70)
    
    # Test with a more general search
    print(f"\n--- Testing organization filter ---")
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    # Try loading some data first
    ckan.load_metadata(
        keywords='data',
        formats=['CSV'],
        limit=10
    )
    
    data = ckan.process_artifacts()
    
    if data:
        # Load into DSI
        for table_name, table_data in data.items():
            if table_data:
                dsi.read(table_data, reader_name='collection', table_name=table_name)
        
        print(f"✓ Loaded {len(data)} tables")
        
        # Check for organizations in the data
        df = dsi.get_table('datasets', collection=True)
        if 'organization_name' in df.columns:
            print(f"\nOrganizations found:")
            print(df['organization_name'].value_counts().head())
    else:
        print(f"✗ No data found")


# ============================================================================
#                    TEST 10: Format Comparison
# ============================================================================
def test_format_comparison():
    """Test loading different formats and compare results"""
    print("\n" + "="*70)
    print("TEST 10: Format Comparison")
    print("="*70)
    
    formats_list = [
        ['CSV'],
        ['JSON'],
        ['CSV', 'JSON']
    ]
    
    results = {}
    
    for formats in formats_list:
        print(f"\n--- Testing formats: {formats} ---")
        
        dsi = DSI()
        ckan = CKAN(
            base_url='https://nationaldataplatform.org/catalog',
            verify_ssl=False
        )
        
        ckan.load_metadata(
            keywords='climate',
            formats=formats,
            limit=10
        )
        
        data = ckan.process_artifacts()
        
        if data:
            # Load into DSI
            for table_name, table_data in data.items():
                if table_data:
                    dsi.read(table_data, reader_name='collection', table_name=table_name)
            
            table_name = list(data.keys())[0]
            df = dsi.get_table(table_name, collection=True)
            results[str(formats)] = {
                'tables': len(data),
                'rows': len(df) if df is not None else 0,
                'cols': len(df.columns) if df is not None else 0
            }
            print(f"  ✓ Loaded: {results[str(formats)]}")
        else:
            results[str(formats)] = None
            print(f"  ✗ No data found")
    
    # Summary
    print("\n--- Summary ---")
    for fmt, result in results.items():
        if result:
            print(f"{fmt}: {result['tables']} tables, {result['rows']} rows")
        else:
            print(f"{fmt}: No data")


# ============================================================================
#                    TEST 11: Complex Query Operations
# ============================================================================
def test_complex_queries():
    """Test complex SQL queries on CKAN data"""
    print("\n" + "="*70)
    print("TEST 11: Complex Query Operations")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV'], limit=20)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    table_name = list(data.keys())[0]
    
    # Complex queries
    queries = [
        f"SELECT COUNT(*) as total FROM {table_name}",
        f"SELECT num_resources FROM {table_name} WHERE num_resources > 2 LIMIT 5"
    ]
    
    for query in queries:
        print(f"\n--- Query: {query} ---")
        try:
            dsi.query(query)
        except Exception as e:
            print(f"Query error: {e}")


# ============================================================================
#                    TEST 12: Data Quality Analysis
# ============================================================================
def test_data_quality():
    """Analyze data quality of CKAN metadata"""
    print("\n" + "="*70)
    print("TEST 12: Data Quality Analysis")
    print("="*70)
    
    dsi = DSI()
    ckan = CKAN(
        base_url='https://nationaldataplatform.org/catalog',
        verify_ssl=False
    )
    
    ckan.load_metadata(keywords='climate', formats=['CSV'], limit=25)
    data = ckan.process_artifacts()
    
    if not data:
        print("No data loaded")
        return
    
    # Load into DSI
    for table_name, table_data in data.items():
        if table_data:
            dsi.read(table_data, reader_name='collection', table_name=table_name)
    
    for table_name in data.keys():
        print(f"\n--- Analyzing table: {table_name} ---")
        
        df = dsi.get_table(table_name, collection=True)
        
        if df is None or df.empty:
            print("  No data to analyze")
            continue
        
        print(f"  Total records: {len(df)}")
        print(f"  Total columns: {len(df.columns)}")
        
        # Check for nulls
        print("\n  Null value analysis:")
        null_counts = df.isnull().sum()
        for col in null_counts[null_counts > 0].index:
            pct = (null_counts[col] / len(df)) * 100
            print(f"    {col}: {null_counts[col]} ({pct:.1f}%)")
        
        # Check data types
        print("\n  Data types:")
        print(df.dtypes.value_counts().to_string())


# ============================================================================
#                            RUN ALL TESTS
# ============================================================================
def run_all_tests():
    """Run all DSI-CKAN tests"""
    tests = [
        test_basic_ckan_backend,
        test_dsi_query_ckan,
        test_dsi_get_table,
        test_dsi_find,
        test_dsi_search,
        test_multiple_ckan_loads,
        test_dsi_write_ckan,
        test_dsi_update_workflow,
        test_organization_filter,
        test_format_comparison,
        test_complex_queries,
        test_data_quality
    ]
    
    print("\n" + "="*70)
    print(" RUNNING ALL DSI-CKAN INTEGRATION TESTS")
    print("="*70)
    
    passed = 0
    failed = 0
    
    for i, test_func in enumerate(tests, 1):
        try:
            test_func()
            passed += 1
            print(f"\n✓ Test {i} ({test_func.__name__}) PASSED")
        except Exception as e:
            failed += 1
            print(f"\n✗ Test {i} ({test_func.__name__}) FAILED: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "-"*70)
    
    print(f"\n{'='*70}")
    print(f"TEST SUMMARY: {passed} passed, {failed} failed out of {len(tests)} total")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        # Remove 'test_' prefix if provided
        if not test_name.startswith('test_'):
            test_name = f'test_{test_name}'
        
        test_func = globals().get(test_name)
        if test_func and callable(test_func):
            test_func()
        else:
            print(f"Test '{test_name}' not found")
            print("\nAvailable tests:")
            for name in sorted(globals().keys()):
                if name.startswith('test_') and callable(globals()[name]):
                    print(f"  - {name}")
    else:
        run_all_tests()


# # Run all tests
# python test_all.py

# # Run specific test
# python test_all.py basic_ckan_backend
# python test_all.py dsi_query_ckan
# python test_all.py format_comparison
# python test_all.py data_quality

# # Run original test
# python test_dsi_with_ckan.py
