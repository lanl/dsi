#!/usr/bin/env python3
"""
CKAN Backend for DSI
Allows DSI to interact with CKAN data catalogs as a backend data source
"""

import requests
import json
import os
import warnings
import certifi
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime

from collections import OrderedDict
from dsi.backends.webserver import Webserver

# Holds table name and data properties
class DataType:
    """
        Primary DataType Artifact class that stores database schema in memory.
        A DataType is a generic construct that defines the schema for the tables inside of SQL. 
        Used to execute CREATE TABLE statements.
    """
    name = ""
    properties = {}
    unit_keys = [] #should be same length as number of keys in properties

class ValueObject:
    """
    Data Structure used when returning search results from ``find``, ``find_table``, ``find_column``, ``find_cell``, or ``find_relation``

        - t_name: table name 
        - c_name: column name as a list. The length of the list varies based on the find function. 
          Read the description of each one to understand the differences
        - row_num: row number. Useful when finding a value in find_cell, find_relation, or find (includes results from find_cell)
        - type: type of match for this specific ValueObject. {table, column, range, cell, row, relation}
    """
    t_name = "" # table name
    c_name = [] # column name(s) 
    row_num = None # row number
    value = None # value stored from that match. Ex: table data, col data, cell data etc.
    type = "" #type of match, {table, column, range, cell, row}

    # implement this later once filesystem table incoroporated into dsi
    # filesystem_match = [] #list of all elements in that matching row in filesystem table


class CKAN(Webserver):
    """
    CKAN Backend to search, browse, and access CKAN data catalog metadata
    
    This backend treats CKAN catalog metadata (datasets and resources) as the primary artifacts,
    storing them in memory as OrderedDicts compatible with DSI middleware.
    """

    def __init__(self, base_url="https://nationaldataplatform.org/catalog",
                 api_key=None, verify_ssl=False, **kwargs):
        """
        Initialize CKAN backend with connection parameters
        
        `base_url` : str
            CKAN instance base URL
        `api_key` : str, optional
            API key for authenticated requests
        `verify_ssl` : bool
            Whether to verify SSL certificates (default: False)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.headers = {'Authorization': api_key} if api_key else {}
        
        # Internal cache for metadata
        self._cache = OrderedDict()
        self._cache['datasets'] = OrderedDict()
        self._cache['resources'] = OrderedDict()
        
        # Track if data has been loaded
        self._loaded = False

    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """
        **Internal use only. Do not call**
        
        Make API request to CKAN
        
        `endpoint` : str
            API endpoint (e.g., 'package_search')
        `params` : dict
            Query parameters
        `return` : dict or None
            JSON response or None on error
        """
        url = f"{self.base_url}/api/3/action/{endpoint}"

        try:
            if self.verify_ssl:
                response = requests.get(url, params=params, headers=self.headers, 
                                      verify=certifi.where())
            else:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
                    response = requests.get(url, params=params, headers=self.headers, 
                                          verify=False)

            response.raise_for_status()
            result = response.json()

            if result.get('success'):
                return result['result']
            else:
                print(f"API Error: {result.get('error', 'Unknown error')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            return None

    def search_datasets(self, keywords: str = None, organization: str = None, 
                       tags: List[str] = None, formats: List[str] = None, 
                       limit: int = 10) -> List[Dict]:
        """
        Search for datasets in CKAN catalog and cache results
        
        `keywords` : str
            Search keywords
        `organization` : str
            Filter by organization name
        `tags` : List[str]
            Filter by tags
        `formats` : List[str]
            Filter by resource formats
        `limit` : int
            Maximum results to return
        `return` : list
            List of dataset dictionaries
        """
        params = {'rows': limit}
        q_parts = []
        fq_parts = []

        if keywords:
            q_parts.append(keywords)
        
        if organization:
            if organization.lower() in ['lanl', 'los-alamos']:
                organization = "los-alamos-national-laboratory"
            fq_parts.append(f'organization:{organization}')

        if tags:
            for tag in tags:
                fq_parts.append(f'tags:{tag}')

        if formats:
            format_queries = [f'res_format:{fmt}' for fmt in formats]
            fq_parts.append('(' + ' OR '.join(format_queries) + ')')

        if q_parts:
            params['q'] = ' '.join(q_parts)
        if fq_parts:
            params['fq'] = ' AND '.join(fq_parts)
        
        result = self._make_request('package_search', params)

        if result:
            total_count = result.get('count', 0)
            datasets = result.get('results', [])
            
            search_parts = []
            if keywords:
                search_parts.append(f"keywords: '{keywords}'")
            if formats:
                search_parts.append(f"formats: {', '.join(formats)}")
            if organization:
                search_parts.append(f"organization: '{organization}'")
            
            search_summary = ', '.join(search_parts) if search_parts else "all datasets"
            print(f"\nFound {total_count} datasets matching {search_summary}. "
                  f"Displaying up to {limit}.")
            
            return datasets
        
        return []

    def load_metadata(self, keywords: str = None, organization: str = None,
                     tags: List[str] = None, formats: List[str] = None,
                     limit: int = 100):
        """
        Load CKAN metadata into backend cache using search parameters
        
        This method populates the internal cache with dataset and resource metadata,
        making it available for DSI operations.
        
        `keywords` : str
            Search keywords
        `organization` : str
            Filter by organization
        `tags` : List[str]
            Filter by tags
        `formats` : List[str]
            Filter by formats
        `limit` : int
            Maximum datasets to load (default: 100)
        """
        datasets = self.search_datasets(
            keywords=keywords,
            organization=organization,
            tags=tags,
            formats=formats,
            limit=limit
        )
        
        if datasets:
            extracted = self._extract_metadata(datasets)
            
            # Convert to OrderedDict format expected by DSI
            self._cache['datasets'] = OrderedDict()
            self._cache['resources'] = OrderedDict()
            
            # Initialize columns
            if extracted['datasets']:
                for key in extracted['datasets'][0].keys():
                    self._cache['datasets'][key] = []
                    
                for dataset in extracted['datasets']:
                    for key, value in dataset.items():
                        self._cache['datasets'][key].append(value)
            
            if extracted['resources']:
                for key in extracted['resources'][0].keys():
                    self._cache['resources'][key] = []
                    
                for resource in extracted['resources']:
                    for key, value in resource.items():
                        self._cache['resources'][key].append(value)
            
            self._loaded = True
            print(f"✓ Loaded {len(extracted['datasets'])} datasets and "
                  f"{len(extracted['resources'])} resources into cache")

    def _extract_metadata(self, datasets: List[Dict]) -> Dict:
        """
        **Internal use only. Do not call**
        
        Extract structured metadata from CKAN datasets
        
        `datasets` : List[Dict]
            List of dataset dictionaries from CKAN API
        `return` : dict
            Dictionary with 'datasets' and 'resources' lists
        """
        datasets_data = []
        resources_data = []
        
        for dataset in datasets:
            dataset_row = {
                'id': dataset.get('id'),
                'name': dataset.get('name'),
                'title': dataset.get('title'),
                'notes': dataset.get('notes', '')[:500] if dataset.get('notes') else '',
                'url': dataset.get('url'),
                'organization_name': (dataset.get('organization', {}).get('name') 
                                     if dataset.get('organization') else None),
                'organization_title': (dataset.get('organization', {}).get('title') 
                                      if dataset.get('organization') else None),
                'author': dataset.get('author'),
                'maintainer': dataset.get('maintainer'),
                'license_id': dataset.get('license_id'),
                'license_title': dataset.get('license_title'),
                'metadata_created': dataset.get('metadata_created'),
                'metadata_modified': dataset.get('metadata_modified'),
                'tags': ', '.join([tag.get('name', '') for tag in dataset.get('tags', [])]),
                'num_resources': dataset.get('num_resources', 0),
                'private': dataset.get('private', False),
                'state': dataset.get('state')
            }
            datasets_data.append(dataset_row)
            
            for resource in dataset.get('resources', []):
                resource_row = {
                    'resource_id': resource.get('id'),
                    'resource_name': resource.get('name'),
                    'format': resource.get('format'),
                    'size': resource.get('size'),
                    'url': resource.get('url'),
                    'description': (resource.get('description', '')[:500] 
                                   if resource.get('description') else ''),
                    'created': resource.get('created'),
                    'last_modified': resource.get('last_modified'),
                    'mimetype': resource.get('mimetype'),
                    'dataset_id': dataset.get('id'),
                    'dataset_name': dataset.get('name'),
                    'dataset_title': dataset.get('title')
                }
                resources_data.append(resource_row)
        
        return {
            'datasets': datasets_data,
            'resources': resources_data
        }

    def ingest_artifacts(self, collection, isVerbose=False):
        """
        Ingest metadata into CKAN backend cache
        
        **Note**: CKAN is primarily a read-only backend for discovery.
        This method loads metadata into the internal cache.
        
        `collection` : OrderedDict
            Nested OrderedDict with 'datasets' and/or 'resources' tables
        `isVerbose` : bool
            If True, prints detailed information
        `return` : None or tuple
            None on success, (ErrorType, message) on error
        """
        if not isinstance(collection, OrderedDict):
            return (TypeError, "collection must be an OrderedDict")
        
        for table_name, table_data in collection.items():
            if table_name not in ['datasets', 'resources']:
                return (ValueError, f"CKAN backend only supports 'datasets' and 'resources' tables, got '{table_name}'")
            
            if not isinstance(table_data, OrderedDict):
                return (TypeError, f"Table '{table_name}' must be an OrderedDict")
            
            self._cache[table_name] = table_data
            
            if isVerbose:
                print(f"Ingested {table_name}: {len(next(iter(table_data.values())))} rows")
        
        self._loaded = True

    def process_artifacts(self, only_units_relations=False):
        """
        Read metadata from CKAN backend cache into OrderedDict
        
        `only_units_relations` : bool
            **USERS SHOULD IGNORE THIS FLAG.** Used internally.
        `return` : OrderedDict
            Nested OrderedDict containing cached CKAN metadata
        """
        if not self._loaded:
            print("WARNING: No metadata loaded. Use load_metadata() or search_datasets() first.")
            return OrderedDict()
        
        return OrderedDict(self._cache)

    def query_artifacts(self, query, isVerbose=False, dict_return=False):
        """
        Query cached CKAN metadata using pandas query syntax
        
        **Note**: Unlike SQL backends, this uses pandas DataFrame queries.
        
        `query` : str
            Pandas query string (e.g., "format == 'CSV'")
        `isVerbose` : bool
            If True, prints query results
        `dict_return` : bool
            If True, returns OrderedDict; if False, returns DataFrame
        `return` : DataFrame or OrderedDict or tuple
            Query results or error tuple
        """
        if not self._loaded:
            return (RuntimeError, "No metadata loaded. Use load_metadata() first.")
        
        try:
            # Determine which table to query based on column names in query
            if any(col in query for col in ['resource_id', 'resource_name', 'dataset_id']):
                df = pd.DataFrame(self._cache['resources'])
            else:
                df = pd.DataFrame(self._cache['datasets'])
            
            result = df.query(query)
            
            if isVerbose:
                print(result)
            
            if dict_return:
                return OrderedDict(result.to_dict(orient='list'))
            return result
            
        except Exception as e:
            return (ValueError, f"Query error: {str(e)}")

    def get_table(self, table_name, dict_return=False):
        """
        Retrieve complete table from cache
        
        `table_name` : str
            Either 'datasets' or 'resources'
        `dict_return` : bool
            If True, returns OrderedDict; if False, returns DataFrame
        `return` : DataFrame or OrderedDict or tuple
        """
        if table_name not in ['datasets', 'resources']:
            return (ValueError, f"Table must be 'datasets' or 'resources', got '{table_name}'")
        
        if not self._loaded:
            return (RuntimeError, "No metadata loaded. Use load_metadata() first.")
        
        if dict_return:
            return OrderedDict(self._cache[table_name])
        return pd.DataFrame(self._cache[table_name])

    def find(self, query_object):
        """
        Search for query_object across all cached metadata
        
        `query_object` : int, float, or str
            Value to search for
        `return` : list or str
            List of ValueObjects or error message
        """
        if not self._loaded:
            return "No metadata loaded. Use load_metadata() first."
        
        table_match = self.find_table(query_object)
        col_match = self.find_column(query_object)
        cell_match = self.find_cell(query_object)
        
        all_return = []
        if isinstance(table_match, list):
            all_return += table_match
        if isinstance(col_match, list):
            all_return += col_match
        if isinstance(cell_match, list):
            all_return += cell_match
            
        if len(all_return) > 0:
            return all_return
        return f"{query_object} was not found in cached metadata"

    def find_table(self, query_object):
        """
        Find tables matching query_object
        
        `query_object` : str
            String to search in table names
        `return` : list or str
            List of ValueObjects or error message
        """
        if not isinstance(query_object, str):
            return f"{query_object} needs to be a string for table name search"
        
        if not self._loaded:
            return "No metadata loaded"
        
        table_list = []
        for table_name in ['datasets', 'resources']:
            if query_object in table_name:
                val = ValueObject()
                val.t_name = table_name
                val.c_name = list(self._cache[table_name].keys())
                val.value = [list(row) for row in zip(*self._cache[table_name].values())]
                val.type = "table"
                table_list.append(val)
        
        if len(table_list) > 0:
            return table_list
        return f"{query_object} not found in table names"

    def find_column(self, query_object, range=False):
        """
        Find columns matching query_object
        
        `query_object` : str
            String to search in column names
        `range` : bool
            If True and column is numeric, return [min, max]
        `return` : list or str
            List of ValueObjects or error message
        """
        if not isinstance(query_object, str):
            return f"{query_object} needs to be a string for column search"
        
        if not self._loaded:
            return "No metadata loaded"
        
        col_list = []
        for table_name in ['datasets', 'resources']:
            for col_name in self._cache[table_name].keys():
                if query_object in col_name:
                    col_data = self._cache[table_name][col_name]
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    
                    if range and all(isinstance(x, (int, float)) or x is None 
                                    for x in col_data):
                        numeric = [x for x in col_data if x is not None]
                        val.value = [min(numeric), max(numeric)] if numeric else [None, None]
                        val.type = "range"
                    else:
                        val.value = col_data
                        val.type = "column"
                    
                    col_list.append(val)
        
        if len(col_list) > 0:
            return col_list
        return f"{query_object} not found in column names"

    def find_cell(self, query_object, row=False):
        """
        Find cells matching query_object
        
        `query_object` : int, float, or str
            Value to search for
        `row` : bool
            If True, return entire matching rows
        `return` : list or str
            List of ValueObjects or error message
        """
        if not self._loaded:
            return "No metadata loaded"
        
        matches = []
        for table_name in ['datasets', 'resources']:
            table = self._cache[table_name]
            col_names = list(table.keys())
            
            for col_name in col_names:
                col_data = table[col_name]
                
                for idx, cell_value in enumerate(col_data):
                    # String matching
                    if isinstance(query_object, str) and isinstance(cell_value, str):
                        if query_object in cell_value:
                            val = ValueObject()
                            val.t_name = table_name
                            val.row_num = idx
                            
                            if row:
                                val.c_name = col_names
                                val.value = [table[c][idx] for c in col_names]
                                val.type = "row"
                            else:
                                val.c_name = [col_name]
                                val.value = cell_value
                                val.type = "cell"
                            
                            matches.append(val)
                    # Exact matching for numbers
                    elif cell_value == query_object:
                        val = ValueObject()
                        val.t_name = table_name
                        val.row_num = idx
                        
                        if row:
                            val.c_name = col_names
                            val.value = [table[c][idx] for c in col_names]
                            val.type = "row"
                        else:
                            val.c_name = [col_name]
                            val.value = cell_value
                            val.type = "cell"
                        
                        matches.append(val)
        
        if len(matches) > 0:
            return matches
        return f"{query_object} not found in any cells"

    def list(self):
        """
        Return list of tables and their dimensions
        
        `return` : list or None
            List of tuples (table_name, num_cols, num_rows)
        """
        if not self._loaded:
            return None
        
        info = []
        for table_name in ['datasets', 'resources']:
            if self._cache[table_name]:
                num_cols = len(self._cache[table_name])
                num_rows = len(next(iter(self._cache[table_name].values())))
                info.append((table_name, num_cols, num_rows))
        
        return info if info else None

    def num_tables(self):
        """Print number of tables in cache"""
        tables = [t for t in ['datasets', 'resources'] if self._cache[t]]
        count = len(tables)
        print(f"Cache has {count} table{'s' if count != 1 else ''}")

    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Display table contents
        
        `table_name` : str
            'datasets' or 'resources'
        `num_rows` : int
            Maximum rows to display
        `display_cols` : list
            Specific columns to display
        `return` : DataFrame or tuple
        """
        if table_name not in ['datasets', 'resources']:
            return (ValueError, f"Table must be 'datasets' or 'resources'")
        
        if not self._loaded or not self._cache[table_name]:
            return (RuntimeError, "No metadata loaded")
        
        df = pd.DataFrame(self._cache[table_name])
        
        if display_cols:
            try:
                df = df[display_cols]
            except KeyError as e:
                return (KeyError, f"Column not found: {e}")
        
        result = df.head(num_rows)
        result.attrs["max_rows"] = len(df)
        return result

    def summary(self, table_name=None):
        """
        Return numerical metadata summary
        
        `table_name` : str, optional
            If specified, return summary for that table only
            If None, return summaries for all tables
        `return` : DataFrame or list or tuple
        """
        if not self._loaded:
            return (RuntimeError, "No metadata loaded")
        
        if table_name is None:
            tables = ['datasets', 'resources']
            summary_list = []
            
            for table in tables:
                if self._cache[table]:
                    headers, rows = self._summary_helper(table)
                    summary_list.append(pd.DataFrame(rows, columns=headers, dtype=object))
            
            summary_list.insert(0, tables)
            return summary_list
        else:
            if table_name not in ['datasets', 'resources']:
                return (ValueError, f"Table must be 'datasets' or 'resources'")
            
            if not self._cache[table_name]:
                return (ValueError, f"'{table_name}' has no data")
            
            headers, rows = self._summary_helper(table_name)
            return pd.DataFrame(rows, columns=headers, dtype=object)

    def _summary_helper(self, table_name):
        """
        **Internal use only. Do not call**
        
        Generate summary metadata for a specific table
        
        `table_name` : str
            'datasets' or 'resources'
        `return` : tuple
            (headers, rows) for DataFrame construction
        """
        table = self._cache[table_name]
        headers = ['column', 'type', 'unique', 'min', 'max', 'avg', 'std_dev']
        rows = []
        
        for col_name, col_data in table.items():
            # Determine type
            if all(isinstance(x, (int, float)) or x is None for x in col_data):
                if all(isinstance(x, int) or x is None for x in col_data):
                    col_type = "INTEGER"
                else:
                    col_type = "FLOAT"
            else:
                col_type = "VARCHAR"
            
            # Count unique values
            unique_vals = len(set(col_data))
            
            # Calculate statistics for numeric columns
            if col_type in ["INTEGER", "FLOAT"]:
                numeric_data = [x for x in col_data if x is not None]
                if numeric_data:
                    min_val = min(numeric_data)
                    max_val = max(numeric_data)
                    avg_val = sum(numeric_data) / len(numeric_data)
                    
                    # Calculate standard deviation
                    if len(numeric_data) > 1:
                        variance = sum((x - avg_val) ** 2 for x in numeric_data) / len(numeric_data)
                        std_dev = variance ** 0.5
                    else:
                        std_dev = 0
                else:
                    min_val = max_val = avg_val = std_dev = None
            else:
                min_val = max_val = avg_val = std_dev = None
            
            rows.append([col_name, col_type, unique_vals, min_val, max_val, avg_val, std_dev])
        
        return headers, rows

    def get_dataset(self, dataset_id: str, verbose: bool = False) -> Optional[Dict]:
        """
        Get detailed information about a specific dataset from CKAN
        
        `dataset_id` : str
            Dataset ID or name
        `verbose` : bool
            If True, print complete JSON
        `return` : dict or None
            Dataset information
        """
        result = self._make_request('package_show', {'id': dataset_id})
        
        if result:
            if verbose:
                print(f"\n{'='*70}")
                print("COMPLETE DATASET METADATA:")
                print(f"{'='*70}")
                print(json.dumps(result, indent=2))
            else:
                print(f"✓ Retrieved dataset: {result.get('title', dataset_id)}")
            return result
        
        return None

    def get_resource(self, resource_id: str, verbose: bool = False) -> Optional[Dict]:
        """
        Get detailed information about a specific resource from CKAN
        
        `resource_id` : str
            Resource ID
        `verbose` : bool
            If True, print complete JSON
        `return` : dict or None
            Resource information
        """
        result = self._make_request('resource_show', {'id': resource_id})
        
        if result:
            if verbose:
                print(f"\n{'='*70}")
                print("COMPLETE RESOURCE METADATA:")
                print(f"{'='*70}")
                print(json.dumps(result, indent=2))
            else:
                print(f"✓ Retrieved resource: {result.get('name', resource_id)}")
            return result
        
        return None

    def download_resource(self, resource_id: str, output_dir: str = "./downloads",
                         filename: str = None) -> Optional[str]:
        """
        Download a resource file from CKAN
        
        `resource_id` : str
            Resource ID to download
        `output_dir` : str
            Directory to save file
        `filename` : str, optional
            Custom filename
        `return` : str or None
            Path to downloaded file or None on error
        """
        # Get resource metadata
        resource = self.get_resource(resource_id)
        if not resource:
            return None
        
        url = resource.get('url')
        if not url:
            print("✗ Resource has no download URL")
            return None
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Determine filename
        if not filename:
            filename = os.path.basename(url) or f"resource_{resource_id}"
            if not os.path.splitext(filename)[1] and resource.get('format'):
                filename += f".{resource['format'].lower()}"
        
        # Clean filename
        filename = "".join(c if c.isalnum() or c in '._-' else '_' 
                          for c in filename)
        
        filepath = os.path.join(output_dir, filename)
        
        # Download
        try:
            print(f"Downloading: {resource.get('name', 'Unnamed')}")
            print(f"  Format: {resource.get('format')}")
            print(f"  Size: {resource.get('size', 'Unknown')}")
            
            if self.verify_ssl:
                response = requests.get(url, headers=self.headers, stream=True,
                                      verify=certifi.where())
            else:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
                    response = requests.get(url, headers=self.headers, stream=True,
                                          verify=False)
            
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"✓ Downloaded to: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"✗ Download failed: {str(e)}")
            return None

    def download_dataset_resources(self, dataset_id: str, output_dir: str = "./downloads",
                                   formats: List[str] = None) -> List[str]:
        """
        Download all resources from a dataset
        
        `dataset_id` : str
            Dataset ID or name
        `output_dir` : str
            Directory to save files
        `formats` : List[str], optional
            List of formats to filter (e.g., ['csv', 'json'])
        `return` : list
            List of downloaded file paths
        """
        dataset = self.get_dataset(dataset_id)
        if not dataset:
            return []
        
        downloaded_files = []
        resources = dataset.get('resources', [])
        
        print(f"\nDataset: {dataset.get('title')}")
        print(f"Found {len(resources)} resources")
        
        for resource in resources:
            # Filter by format if specified
            resource_format = resource.get('format', '').lower()
            if formats and resource_format not in [f.lower() for f in formats]:
                print(f"  Skipping {resource.get('name')} ({resource_format})")
                continue
            
            # Download resource
            filepath = self.download_resource(
                resource['id'],
                output_dir=output_dir
            )
            
            if filepath:
                downloaded_files.append(filepath)
        
        print(f"\n✓ Downloaded {len(downloaded_files)} resources")
        return downloaded_files

    def export_to_csv(self, output_file: str, include_resources: bool = True) -> bool:
        """
        Export cached metadata to CSV files
        
        `output_file` : str
            Output CSV filename for datasets
        `include_resources` : bool
            If True, create separate CSV for resources
        `return` : bool
            True if successful
        """
        if not self._loaded:
            print("✗ No metadata loaded to export")
            return False
        
        try:
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            
            # Export datasets
            df_datasets = pd.DataFrame(self._cache['datasets'])
            df_datasets.to_csv(output_file, index=False)
            print(f"✓ Exported datasets to: {output_file}")
            
            # Export resources if requested
            if include_resources and self._cache['resources']:
                resource_file = output_file.replace('.csv', '_resources.csv')
                df_resources = pd.DataFrame(self._cache['resources'])
                df_resources.to_csv(resource_file, index=False)
                print(f"✓ Exported resources to: {resource_file}")
            
            return True
            
        except Exception as e:
            print(f"✗ Export failed: {str(e)}")
            return False

    def export_to_json(self, output_file: str, pretty: bool = True) -> bool:
        """
        Export complete cached metadata to JSON
        
        `output_file` : str
            Output JSON filename
        `pretty` : bool
            If True, format with indentation
        `return` : bool
            True if successful
        """
        if not self._loaded:
            print("✗ No metadata loaded to export")
            return False
        
        try:
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            
            export_data = {
                'export_date': datetime.now().isoformat(),
                'base_url': self.base_url,
                'datasets_count': len(next(iter(self._cache['datasets'].values()))) if self._cache['datasets'] else 0,
                'resources_count': len(next(iter(self._cache['resources'].values()))) if self._cache['resources'] else 0,
                'datasets': self._cache['datasets'],
                'resources': self._cache['resources']
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(export_data, f, ensure_ascii=False)
            
            print(f"✓ Exported metadata to JSON: {output_file}")
            return True
            
        except Exception as e:
            print(f"✗ JSON export failed: {str(e)}")
            return False

    def display_datasets(self, datasets: List[Dict] = None, detailed: bool = False, 
                        verbose: bool = False):
        """
        Display dataset information in readable format
        
        `datasets` : List[Dict], optional
            List of dataset dictionaries (if None, uses cached data)
        `detailed` : bool
            Show detailed information
        `verbose` : bool
            Show complete JSON for each dataset
        """
        if datasets is None:
            if not self._loaded or not self._cache['datasets']:
                print("\nNo datasets to display.")
                return
            # Convert cached data back to list of dicts for display
            num_datasets = len(next(iter(self._cache['datasets'].values())))
            datasets = []
            for i in range(num_datasets):
                dataset = {k: v[i] for k, v in self._cache['datasets'].items()}
                datasets.append(dataset)
        
        if not datasets:
            print("\nNo datasets to display.")
            return
        
        for i, dataset in enumerate(datasets, 1):
            print(f"\n{'='*70}")
            print(f"--- Dataset {i}: {dataset.get('name', 'unnamed')} ---")
            print(f"{'='*70}")
            
            if verbose:
                print(json.dumps(dataset, indent=2))
            else:
                print(f"Title: {dataset.get('title', 'N/A')}")
                
                if dataset.get('notes'):
                    notes = dataset['notes']
                    if len(notes) > 300:
                        notes = notes[:300] + '...'
                    print(f"Description: {notes}")
                
                print(f"Organization: {dataset.get('organization_title', 'N/A')}")
                print(f"Number of Resources: {dataset.get('num_resources', 0)}")
                
                if detailed:
                    print(f"Author: {dataset.get('author', 'N/A')}")
                    print(f"Maintainer: {dataset.get('maintainer', 'N/A')}")
                    print(f"License: {dataset.get('license_title', 'N/A')}")
                    print(f"Created: {dataset.get('metadata_created', 'N/A')}")
                    print(f"Modified: {dataset.get('metadata_modified', 'N/A')}")
                    print(f"Tags: {dataset.get('tags', 'N/A')}")

    def get_statistics(self) -> Dict:
        """
        Get cached statistics
        
        `return` : dict
            Statistics about cached data
        """
        if not self._loaded:
            return {
                'datasets': 0,
                'resources': 0,
                'loaded': False
            }
        
        return {
            'datasets': len(next(iter(self._cache['datasets'].values()))) if self._cache['datasets'] else 0,
            'resources': len(next(iter(self._cache['resources'].values()))) if self._cache['resources'] else 0,
            'loaded': True,
            'base_url': self.base_url
        }

    def display_statistics(self):
        """Display cached statistics in readable format"""
        stats = self.get_statistics()
        
        print(f"\n{'='*60}")
        print("CKAN BACKEND STATISTICS")
        print(f"{'='*60}")
        print(f"Base URL: {stats.get('base_url', 'N/A')}")
        print(f"Metadata Loaded: {stats['loaded']}")
        print(f"Cached Datasets: {stats['datasets']}")
        print(f"Cached Resources: {stats['resources']}")
        print()

    def notebook(self, interactive=False):
        """
        Generate Jupyter notebook displaying cached CKAN metadata
        
        `interactive` : bool
            If True, opens Jupyter Lab; if False, creates HTML file
        `return` : None
        """
        import nbconvert as nbc
        import nbformat as nbf
        import textwrap
        
        if not self._loaded:
            print("ERROR: No metadata loaded. Use load_metadata() first.")
            return
        
        nb = nbf.v4.new_notebook()
        
        text = """\
        This notebook was auto-generated by DSI CKAN Backend.
        It displays metadata from a CKAN data catalog including datasets and resources.
        Execute the cells below to explore the data.
        """
        
        code1 = """\
        import pandas as pd
        from collections import OrderedDict
        """
        
        # Prepare data for notebook
        datasets_dict = dict(self._cache['datasets'])
        resources_dict = dict(self._cache['resources'])
        
        code2 = f"""\
        # Load cached CKAN metadata
        datasets_data = {datasets_dict}
        resources_data = {resources_dict}
        
        df_datasets = pd.DataFrame(datasets_data)
        df_resources = pd.DataFrame(resources_data)
        
        df_datasets.attrs['name'] = 'datasets'
        df_resources.attrs['name'] = 'resources'
        
        print(f"Loaded {{len(df_datasets)}} datasets and {{len(df_resources)}} resources")
        """
        
        code3 = """\
        # Display datasets
        print("\\n" + "="*70)
        print("DATASETS")
        print("="*70)
        print(df_datasets)
        print(f"\\nShape: {df_datasets.shape}")
        """
        
        code4 = """\
        # Display resources
        print("\\n" + "="*70)
        print("RESOURCES")
        print("="*70)
        print(df_resources)
        print(f"\\nShape: {df_resources.shape}")
        """
        
        code5 = """\
        # Summary statistics
        print("\\n" + "="*70)
        print("DATASET SUMMARY")
        print("="*70)
        print(df_datasets.describe(include='all'))
        
        print("\\n" + "="*70)
        print("RESOURCE SUMMARY")
        print("="*70)
        print(df_resources.describe(include='all'))
        """
        
        nb['cells'] = [
            nbf.v4.new_markdown_cell(textwrap.dedent(text)),
            nbf.v4.new_code_cell(textwrap.dedent(code1)),
            nbf.v4.new_code_cell(textwrap.dedent(code2)),
            nbf.v4.new_code_cell(textwrap.dedent(code3)),
            nbf.v4.new_code_cell(textwrap.dedent(code4)),
            nbf.v4.new_code_cell(textwrap.dedent(code5))
        ]
        
        fname = 'dsi_ckan_backend_output.ipynb'
        print('Writing Jupyter notebook...')
        with open(fname, 'w') as fh:
            nbf.write(nb, fh)
        
        # Execute notebook
        with open(fname, 'r', encoding='utf-8') as fh:
            nb_content = nbf.read(fh, as_version=4)
        
        run_nb = nbc.preprocessors.ExecutePreprocessor(timeout=-1)
        run_nb.preprocess(nb_content, {'metadata': {'path': '.'}})
        
        if interactive:
            print('Opening Jupyter notebook...')
            import subprocess
            proc = subprocess.run(['jupyter-lab', fname], capture_output=True)
            if proc.stderr:
                print(f"Error: {proc.stderr.decode()}")
        else:
            # Export to HTML
            html_exporter = nbc.HTMLExporter()
            html_content, _ = html_exporter.from_notebook_node(nb_content)
            
            html_filename = 'dsi_ckan_backend_output.html'
            with open(html_filename, 'w', encoding='utf-8') as fh:
                fh.write(html_content)
            
            print(f'✓ Created HTML output: {html_filename}')

    def close(self):
        """
        Close CKAN backend connection
        
        Clears the cache and resets the loaded state.
        """
        self._cache = OrderedDict()
        self._cache['datasets'] = OrderedDict()
        self._cache['resources'] = OrderedDict()
        self._loaded = False
        print("✓ CKAN backend closed and cache cleared")