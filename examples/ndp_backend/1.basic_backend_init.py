#!/usr/bin/env python3
"""
CKAN Backend Test Suite
Demonstrates how to use CKAN backend with DSI for catalog discovery and metadata management

# Run all tests
python test_ckan_backend.py

# Run specific test
python test_ckan_backend.py --test 3

# Keep generated files for inspection
python test_ckan_backend.py --keep-files
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