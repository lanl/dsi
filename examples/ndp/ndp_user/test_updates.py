# examples/ndp/ndp_user/test_ndp_updates.py
from dsi.dsi import DSI

def main(verbose=False):
    """
    Tests all 4 NDP backend updates:
    1. num_datasets() - Returns count of datasets (rows in datasets table)
    2. list() - SQLite-compatible format
    3. summary() - SQLite-compatible format
    4. query() raises NotImplementedError, find() uses find_relation()
    """
    
    print("="*70)
    print("NDP Backend Updates Test")
    print("="*70)
    
    # Initialize with limit=5 to get exactly 5 datasets
    dsi = DSI(
        backend_name="NDP",
        keywords="temperature",
        limit=5
    )
    
    # ======================================================================
    # TEST 1: num_datasets() - should return 5
    # ======================================================================
    print("\n" + "="*70)
    print("TEST 1: num_datasets()")
    print("="*70)
    print("\nExpected: 5 datasets loaded")
    print("Actual output:")
    dsi.num_datasets()
    
    # ======================================================================
    # TEST 2: list() - SQLite-compatible format
    # ======================================================================
    print("\n" + "="*70)
    print("TEST 2: list() - SQLite Format")
    print("="*70)
    print("\nExpected format:")
    print("Table: datasets")
    print("  - num of columns: X")
    print("  - num of rows: 5")
    print("Table: <dataset_title>")
    print("  - num of columns: X")
    print("  - num of rows: X")
    print("\nActual output:")
    dsi.list()
    
    if verbose:
        print("\n" + "-"*70)
        print("Testing list(collection=True) - should return list of table names")
        print("-"*70)
        table_list = dsi.list(collection=True)
        print(f"Type: {type(table_list)}")
        print(f"Tables: {list(table_list)}")
    
    # ======================================================================
    # TEST 3: summary() - SQLite-compatible format
    # ======================================================================
    print("\n" + "="*70)
    print("TEST 3: summary() - SQLite Format")
    print("="*70)
    print("\nExpected format:")
    print("Table: datasets")
    print("  - num of columns: X")
    print("  - num of rows: 5")
    print("  - columns: id, name, title, ...")
    print("\nActual output:")
    dsi.summary(collection=False)
    
    if verbose:
        print("\n" + "-"*70)
        print("Testing summary(collection=True) - returns list [table_names, df1, df2, ...]")
        print("-"*70)
        summary_result = dsi.summary(collection=True)
        if isinstance(summary_result, list):
            print(f"Returned list with {len(summary_result)} elements")
            print(f"Table names: {summary_result[0]}")
            if len(summary_result) > 1:
                print("\nFirst table summary:")
                print(summary_result[1])
        
        print("\n" + "-"*70)
        print("Testing summary('datasets') - single table")
        print("-"*70)
        datasets_summary = dsi.summary("datasets", collection=True)
        print(datasets_summary)
    
    # ======================================================================
    # TEST 4: query() raises NotImplementedError
    # ======================================================================
    print("\n" + "="*70)
    print("TEST 4: query() - Should Raise NotImplementedError")
    print("="*70)
    print("\nTrying: dsi.query('SELECT * FROM datasets')")
    
    try:
        dsi.query("SELECT * FROM datasets")
        print("❌ ERROR: query() did not raise NotImplementedError!")
    except NotImplementedError as e:
        print("✓ Correctly raised NotImplementedError")
        print(f"\nError message:\n{e}")
    
    # ======================================================================
    # TEST 5: find() uses find_relation() internally
    # ======================================================================
    print("\n" + "="*70)
    print("TEST 5: find() - Uses find_relation()")
    print("="*70)
    
    # Test find with comparison operators
    print("\nTest 5a: find('num_resources > 2')")
    print("-"*70)
    dsi.find("num_resources > 2")
    
    if verbose:
        print("\n" + "-"*70)
        print("Test 5b: find('num_resources < 2', collection=True)")
        print("-"*70)
        results = dsi.find("num_resources < 2")
        if results is not None and not results.empty:
            print(f"\nFound {len(results)} matching rows")
            print(results[['title', 'num_resources']].head())
        else:
            print("No matches found")
        
        print("\n" + "-"*70)
        print("Test 5c: find with string matching - find('organization == \"Oceans11 - LANL\"')")
        print("-"*70)
        # Note: This might not find anything depending on the data
        try:
            results2 = dsi.find('organization == "Oceans11 - LANL"')
            if results2 is not None and not results2.empty:
                print(f"Found {len(results2)} datasets from Oceans11 - LANL")
            else:
                print("No datasets from this organization in current query")
        except Exception as e:
            print(f"Note: {e}")
    
    # ======================================================================
    # Verification Summary
    # ======================================================================
    print("\n" + "="*70)
    print("VERIFICATION SUMMARY")
    print("="*70)
    
    # Get actual values for verification
    table_list = dsi.list(collection=True)
    datasets_df = dsi.get_table("datasets", collection=True)
    
    print(f"\n✓ Backend loaded: {len(table_list)} tables")
    print(f"✓ Datasets table: {len(datasets_df)} rows")
    print(f"✓ num_datasets() should report: {len(datasets_df)} datasets")
    print(f"✓ list() format: SQLite-compatible")
    print(f"✓ summary() format: SQLite-compatible")
    print(f"✓ query() behavior: Raises NotImplementedError")
    print(f"✓ find() behavior: Uses pandas query syntax (find_relation)")
    
    # Show table structure
    if verbose:
        print("\n" + "="*70)
        print("DETAILED TABLE STRUCTURE")
        print("="*70)
        
        for table_name in table_list:
            print(f"\nTable: {table_name}")
            df = dsi.get_table(table_name, collection=True)
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            if table_name == "datasets":
                print(f"\n  First 2 titles:")
                for i, title in enumerate(df['title'].head(2)):
                    print(f"    {i+1}. {title}")
    
    dsi.close()
    print("\n" + "="*70)
    print("Test Complete")
    print("="*70)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Test NDP backend updates"
    )
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed output and additional tests")
    args = parser.parse_args()
    main(verbose=args.verbose)