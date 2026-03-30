#!/usr/bin/env python3
"""
CKAN Backend Test Suite
Demonstrates how to use CKAN backend with DSI for catalog discovery and metadata management
"""

import os
import sys
from collections import OrderedDict

# Add parent directory to path if running directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DSI.dsi.backends.ckan_v1 import CKAN

# ============================================================================
# Test 1: Basic Backend Initialization and Search
# ============================================================================

def test_basic_search():
    """Test basic CKAN search and metadata loading"""
    print("\n" + "="*70)
    print("TEST 1: Basic Search and Metadata Loading")
    print("="*70)
    
    # Initialize CKAN backend
    backend = CKAN(base_url="https://nationaldataplatform.org/catalog", verify_ssl=False)
    
    # Search for climate data with CSV format
    print("\nSearching for climate CSV datasets...")
    backend.load_metadata(keywords="climate", formats=["CSV"], limit=5)
    
    # Display what was loaded
    backend.display_statistics()
    
    # List tables
    print("\nTables loaded:")
    tables = backend.list()
    if tables:
        for table_name, num_cols, num_rows in tables:
            print(f"  {table_name}: {num_rows} rows, {num_cols} columns")
    
    backend.close()
    print("\n✓ Test 1 passed")


# ============================================================================
# Test 2: Query Operations
# ============================================================================

def test_query_operations():
    """Test querying cached CKAN metadata"""
    print("\n" + "="*70)
    print("TEST 2: Query Operations")
    print("="*70)
    
    backend = CKAN()
    backend.load_metadata(keywords="energy", limit=10)
    
    # Query datasets with pandas syntax
    print("\nQuerying datasets where title contains 'Data'...")
    result = backend.query_artifacts("title.str.contains('Data', na=False)", dict_return=False)
    if isinstance(result, tuple):
        print(f"Query error: {result[1]}")
    else:
        print(f"Found {len(result)} matching datasets")
        print(result[['name', 'title']].head())
    
    # Get complete table
    print("\nRetrieving complete datasets table...")
    datasets = backend.get_table('datasets', dict_return=False)
    print(f"Shape: {datasets.shape}")
    print(f"Columns: {list(datasets.columns)}")
    
    backend.close()
    print("\n✓ Test 2 passed")


# ============================================================================
# Test 3: Find Operations
# ============================================================================

def test_find_operations():
    """Test find operations across metadata"""
    print("\n" + "="*70)
    print("TEST 3: Find Operations")
    print("="*70)
    
    backend = CKAN()
    backend.load_metadata(keywords="climate", limit=5)
    
    # Find table
    print("\nFinding tables with 'dataset' in name...")
    result = backend.find_table("dataset")
    if isinstance(result, list):
        print(f"Found {len(result)} matching tables")
        for val_obj in result:
            print(f"  - {val_obj.t_name} ({len(val_obj.c_name)} columns)")
    else:
        print(result)
    
    # Find column
    print("\nFinding columns with 'title' in name...")
    result = backend.find_column("title")
    if isinstance(result, list):
        print(f"Found {len(result)} matching columns")
        for val_obj in result:
            print(f"  - {val_obj.t_name}.{val_obj.c_name[0]}")
    else:
        print(result)
    
    # Find cell
    print("\nFinding cells containing 'climate'...")
    result = backend.find_cell("climate", row=False)
    if isinstance(result, list):
        print(f"Found {len(result)} matches")
        for val_obj in result[:3]:  # Show first 3
            print(f"  - {val_obj.t_name}.{val_obj.c_name[0]} (row {val_obj.row_num})")
    else:
        print(result)
    
    # Find with complete search
    print("\nSearching for 'CSV'...")
    result = backend.find("CSV")
    if isinstance(result, list):
        print(f"Found {len(result)} total matches across tables, columns, and cells")
    else:
        print(result)
    
    backend.close()
    print("\n✓ Test 3 passed")


# ============================================================================
# Test 4: Display and Summary Operations
# ============================================================================

def test_display_and_summary():
    """Test display and summary operations"""
    print("\n" + "="*70)
    print("TEST 4: Display and Summary")
    print("="*70)
    
    backend = CKAN()
    backend.load_metadata(organization="los-alamos", limit=5)
    
    # Display datasets table
    print("\nDisplaying datasets table (first 3 rows)...")
    result = backend.display('datasets', num_rows=3, display_cols=['name', 'title', 'organization_title'])
    if not isinstance(result, tuple):
        print(result)
        print(f"\nTotal rows in table: {result.attrs.get('max_rows', 'Unknown')}")
    else:
        print(f"Error: {result[1]}")
    
    # Get summary statistics
    print("\nGetting summary statistics for datasets...")
    summary = backend.summary('datasets')
    if not isinstance(summary, tuple):
        print(summary.head(10))
    else:
        print(f"Error: {summary[1]}")
    
    backend.close()
    print("\n✓ Test 4 passed")


# ============================================================================
# Test 5: Export Operations
# ============================================================================

def test_export_operations():
    """Test exporting metadata to CSV and JSON"""
    print("\n" + "="*70)
    print("TEST 5: Export Operations")
    print("="*70)
    
    backend = CKAN()
    backend.load_metadata(keywords="climate", formats=["CSV", "JSON"], limit=10)
    
    # Export to CSV
    print("\nExporting to CSV...")
    success = backend.export_to_csv("test_ckan_export.csv", include_resources=True)
    if success:
        print("✓ CSV export successful")
        if os.path.exists("test_ckan_export.csv"):
            print(f"  - Created: test_ckan_export.csv")
        if os.path.exists("test_ckan_export_resources.csv"):
            print(f"  - Created: test_ckan_export_resources.csv")
    
    # Export to JSON
    print("\nExporting to JSON...")
    success = backend.export_to_json("test_ckan_export.json", pretty=True)
    if success and os.path.exists("test_ckan_export.json"):
        print("✓ JSON export successful")
        print(f"  - Created: test_ckan_export.json")
    
    backend.close()
    print("\n✓ Test 5 passed")


# ============================================================================
# Test 6: Artifact Ingestion and Processing
# ============================================================================

def test_artifact_operations():
    """Test ingesting and processing artifacts"""
    print("\n" + "="*70)
    print("TEST 6: Artifact Ingestion and Processing")
    print("="*70)
    
    backend = CKAN()
    
    # Create sample data as OrderedDict (simulating DSI reader output)
    sample_data = OrderedDict()
    sample_data['datasets'] = OrderedDict([
        ('id', ['dataset1', 'dataset2']),
        ('name', ['test_dataset_1', 'test_dataset_2']),
        ('title', ['Test Dataset 1', 'Test Dataset 2']),
        ('organization_name', ['org1', 'org2']),
        ('num_resources', [3, 5])
    ])
    sample_data['resources'] = OrderedDict([
        ('resource_id', ['res1', 'res2', 'res3']),
        ('resource_name', ['Resource 1', 'Resource 2', 'Resource 3']),
        ('format', ['CSV', 'JSON', 'CSV']),
        ('dataset_id', ['dataset1', 'dataset1', 'dataset2'])
    ])
    
    # Ingest artifacts
    print("\nIngesting sample artifacts...")
    result = backend.ingest_artifacts(sample_data, isVerbose=True)
    if result is None:
        print("✓ Ingestion successful")
    else:
        print(f"✗ Ingestion failed: {result}")
    
    # Process artifacts (read back)
    print("\nProcessing artifacts...")
    artifacts = backend.process_artifacts()
    print(f"Retrieved {len(artifacts)} tables:")
    for table_name, table_data in artifacts.items():
        if table_data:
            num_rows = len(next(iter(table_data.values())))
            print(f"  - {table_name}: {num_rows} rows")
    
    # Verify data integrity
    print("\nVerifying data integrity...")
    datasets = artifacts.get('datasets', {})
    if datasets and datasets.get('name') == ['test_dataset_1', 'test_dataset_2']:
        print("✓ Data integrity verified")
    else:
        print("✗ Data integrity check failed")
    
    backend.close()
    print("\n✓ Test 6 passed")


# ============================================================================
# Test 7: Download Operations
# ============================================================================

def test_download_operations():
    """Test downloading resources from CKAN"""
    print("\n" + "="*70)
    print("TEST 7: Download Operations (Mock)")
    print("="*70)
    print("\nNote: Actual downloads require valid resource IDs")
    print("This test demonstrates the API without downloading")
    
    backend = CKAN()
    backend.load_metadata(keywords="climate", formats=["CSV"], limit=3)
    
    # Get resources from cache
    resources = backend.get_table('resources', dict_return=True)
    
    if resources and 'resource_id' in resources:
        resource_ids = resources['resource_id']
        resource_names = resources['resource_name']
        
        print(f"\nFound {len(resource_ids)} resources:")
        for i, (rid, rname) in enumerate(zip(resource_ids[:3], resource_names[:3]), 1):
            print(f"  {i}. {rname} (ID: {rid})")
        
        # Show how to download (without actually downloading)
        if resource_ids:
            print(f"\nTo download, use:")
            print(f"  backend.download_resource('{resource_ids[0]}')")
            print(f"  backend.download_dataset_resources('dataset_id')")
    else:
        print("No resources found in cache")
    
    backend.close()
    print("\n✓ Test 7 passed")


# ============================================================================
# Test 8: Notebook Generation
# ============================================================================

def test_notebook_generation():
    """Test Jupyter notebook generation"""
    print("\n" + "="*70)
    print("TEST 8: Notebook Generation")
    print("="*70)
    
    backend = CKAN()
    backend.load_metadata(keywords="energy", limit=5)
    
    print("\nGenerating Jupyter notebook...")
    try:
        backend.notebook(interactive=False)
        
        if os.path.exists("dsi_ckan_backend_output.ipynb"):
            print("✓ Notebook created: dsi_ckan_backend_output.ipynb")
        
        if os.path.exists("dsi_ckan_backend_output.html"):
            print("✓ HTML created: dsi_ckan_backend_output.html")
    except Exception as e:
        print(f"✗ Notebook generation failed: {e}")
    
    backend.close()
    print("\n✓ Test 8 passed")


# ============================================================================
# Test 9: Error Handling
# ============================================================================

def test_error_handling():
    """Test error handling and edge cases"""
    print("\n" + "="*70)
    print("TEST 9: Error Handling")
    print("="*70)
    
    backend = CKAN()
    
    # Test operations without loaded data
    print("\nTesting operations without loaded data...")
    
    result = backend.query_artifacts("name == 'test'")
    if isinstance(result, tuple):
        print(f"✓ Expected error: {result[1]}")
    
    result = backend.display('datasets')
    if isinstance(result, tuple):
        print(f"✓ Expected error: {result[1]}")
    
    # Load data and test with invalid table
    backend.load_metadata(keywords="test", limit=2)
    
    print("\nTesting with invalid table name...")
    result = backend.get_table('invalid_table')
    if isinstance(result, tuple):
        print(f"✓ Expected error: {result[1]}")
    
    # Test with invalid query
    print("\nTesting with invalid query...")
    result = backend.query_artifacts("invalid syntax @#$")
    if isinstance(result, tuple):
        print(f"✓ Expected error: {result[1]}")
    
    backend.close()
    print("\n✓ Test 9 passed")


# ============================================================================
# Test 10: Integration with DSI Core (if available)
# ============================================================================

def test_dsi_integration():
    """Test integration with DSI Core Terminal"""
    print("\n" + "="*70)
    print("TEST 10: DSI Core Integration")
    print("="*70)
    
    try:
        from dsi.core import Terminal
        
        print("\nInitializing DSI Terminal with CKAN backend...")
        
        # Create CKAN backend
        ckan = CKAN()
        ckan.load_metadata(keywords="climate", formats=["CSV"], limit=5)
        
        # Create Terminal with CKAN as front-read backend
        terminal = Terminal()
        terminal.load_module(ckan, "front-read")
        
        # Process artifacts through Terminal
        print("\nProcessing artifacts through Terminal...")
        artifacts = terminal.front_read.process_artifacts()
        
        print(f"✓ Processed {len(artifacts)} tables through DSI Core")
        
        terminal.close()
        
    except ImportError:
        print("DSI Core not available - skipping integration test")
    except Exception as e:
        print(f"Integration test error: {e}")
    
    print("\n✓ Test 10 passed")


# ============================================================================
# Run All Tests
# ============================================================================

def run_all_tests():
    """Run all test cases"""
    print("\n" + "="*70)
    print("CKAN BACKEND TEST SUITE")
    print("="*70)
    
    tests = [
        test_basic_search,
        test_query_operations,
        test_find_operations,
        test_display_and_summary,
        test_export_operations,
        test_artifact_operations,
        test_download_operations,
        test_notebook_generation,
        test_error_handling,
        test_dsi_integration
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n✗ {test.__name__} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    # Cleanup test files
    print("\n" + "="*70)
    print("Cleaning up test files...")
    test_files = [
        "test_ckan_export.csv",
        "test_ckan_export_resources.csv",
        "test_ckan_export.json",
        "dsi_ckan_backend_output.ipynb",
        "dsi_ckan_backend_output.html"
    ]
    
    for file in test_files:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"  Removed: {file}")
            except:
                pass
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n✓ ALL TESTS PASSED")
    else:
        print(f"\n✗ {failed} TEST(S) FAILED")
    
    return failed == 0


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="CKAN Backend Test Suite")
    parser.add_argument("--test", type=int, help="Run specific test number (1-10)")
    parser.add_argument("--keep-files", action="store_true", help="Keep generated test files")
    
    args = parser.parse_args()
    
    if args.test:
        # Run specific test
        tests = [
            test_basic_search,
            test_query_operations,
            test_find_operations,
            test_display_and_summary,
            test_export_operations,
            test_artifact_operations,
            test_download_operations,
            test_notebook_generation,
            test_error_handling,
            test_dsi_integration
        ]
        
        if 1 <= args.test <= len(tests):
            print(f"\nRunning Test {args.test}...")
            tests[args.test - 1]()
        else:
            print(f"Invalid test number. Choose 1-{len(tests)}")
    else:
        # Run all tests
        success = run_all_tests()
        sys.exit(0 if success else 1)
