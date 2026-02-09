# from dsi import DSI

# # Initialize with NDP backend
# dsi = DSI(backend_name="NDP", 
#           base_url="https://nationaldataplatform.org/catalog")

# # Use dsi.find() to search
# results = dsi.find("format = CSV", collection=True)

# # Or search with parameters
# dsi.read(
#     filenames={'keywords': 'climate', 'formats': ['CSV'], 'limit': 5},
#     reader_name='NDP_Search'
# )

# # Use DSI's find methods
# climate_data = dsi.find("title ~ climate", collection=True)

# # Query by organization
# lanl_datasets = dsi.find("organization = LANL", collection=True)

# # Close when done
# dsi.close()

"""
NDP (National Data Platform) Backend for DSI
Provides read-only access to CKAN catalog via API
"""

import requests
import urllib3
from collections import OrderedDict
from dsi.backends.filesystem import Backend

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NDP(Backend):
    """
    National Data Platform (CKAN) Backend
    
    Provides read-only access to CKAN catalog metadata and resources.
    """
    
    def __init__(self, base_url="https://nationaldataplatform.org/catalog", 
                 api_key=None, verify_ssl=False):
        """
        Initializes an NDP backend
        
        Parameters
        ----------
        base_url : str
            Base URL of the CKAN instance
        api_key : str, optional
            API key for authenticated requests
        verify_ssl : bool, default False
            Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({'Authorization': self.api_key})
        
        # Store fetched datasets/resources in memory
        self.datasets = OrderedDict()
        self.resources = OrderedDict()
        self.last_search_results = []
        
    def _make_request(self, endpoint, params=None):
        """Make API request to CKAN"""
        url = f"{self.base_url}/api/3/action/{endpoint}"
        try:
            response = self.session.get(url, params=params, verify=self.verify_ssl)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('success'):
                raise Exception(f"CKAN API error: {data.get('error', {})}")
            
            return data.get('result', {})
        
        except requests.RequestException as e:
            raise Exception(f"NDP Backend request failed: {e}")
    
    def search_datasets(self, keywords=None, organization=None, tags=None, 
                       formats=None, limit=10):
        """
        Search for datasets in CKAN catalog
        
        Returns list of dataset dictionaries
        """
        params = {'rows': limit}
        
        # Build search query
        query_parts = []
        if keywords:
            query_parts.append(keywords)
        if organization:
            query_parts.append(f'organization:{organization}')
        if tags:
            for tag in tags:
                query_parts.append(f'tags:{tag}')
        if formats:
            format_query = ' OR '.join([f'res_format:{fmt}' for fmt in formats])
            query_parts.append(f'({format_query})')
        
        if query_parts:
            params['q'] = ' '.join(query_parts)
        
        result = self._make_request('package_search', params)
        
        datasets = result.get('results', [])
        self.last_search_results = datasets
        
        # Store in internal cache
        for dataset in datasets:
            self.datasets[dataset['id']] = dataset
            for resource in dataset.get('resources', []):
                self.resources[resource['id']] = resource
        
        return datasets
    
    def get_dataset(self, dataset_id):
        """Fetch a specific dataset by ID or name"""
        if dataset_id in self.datasets:
            return self.datasets[dataset_id]
        
        result = self._make_request('package_show', {'id': dataset_id})
        self.datasets[dataset_id] = result
        
        for resource in result.get('resources', []):
            self.resources[resource['id']] = resource
        
        return result
    
    def get_resource(self, resource_id):
        """Fetch a specific resource by ID"""
        if resource_id in self.resources:
            return self.resources[resource_id]
        
        result = self._make_request('resource_show', {'id': resource_id})
        self.resources[resource_id] = result
        return result
    
    # ========== DSI Backend Interface Methods ==========
    
    def ingest_artifacts(self, collection, isVerbose=False):
        """
        Load search results into DSI tables
        
        Parameters
        ----------
        collection : dict
            Search parameters: keywords, organization, formats, tags, limit
        """
        # Extract search params
        keywords = collection.get('keywords')
        organization = collection.get('organization')
        tags = collection.get('tags')
        formats = collection.get('formats')
        limit = collection.get('limit', 10)
        
        # Perform search
        datasets = self.search_datasets(
            keywords=keywords,
            organization=organization,
            tags=tags,
            formats=formats,
            limit=limit
        )
        
        if isVerbose:
            print(f"Found {len(datasets)} datasets")
        
        return True
    
    def query_artifacts(self, query, kwargs):
        """
        Query cached datasets/resources
        
        query : str
            Search term or dataset/resource ID
        """
        # Try as dataset ID first
        if query in self.datasets:
            return self.datasets[query]
        
        # Try as resource ID
        if query in self.resources:
            return self.resources[query]
        
        # Search in metadata
        results = []
        for dataset in self.datasets.values():
            if query.lower() in str(dataset).lower():
                results.append(dataset)
        
        return results
    
    def find(self, query_object, kwargs):
        """
        Find rows matching query_object condition
        
        query_object : str
            Format: "column_name operator value"
            Example: "format = CSV"
        """
        from dsi.core import Value
        
        # Parse query: "column operator value"
        parts = query_object.split()
        if len(parts) < 3:
            raise ValueError("Query must be: 'column operator value'")
        
        column = parts[0]
        operator = parts[1]
        value = ' '.join(parts[2:]).strip('"\'')
        
        matches = []
        
        # Search in datasets
        for idx, (dataset_id, dataset) in enumerate(self.datasets.items()):
            if self._matches_condition(dataset, column, operator, value):
                # Create DSI Value object
                match = Value(
                    t_name='ndp_datasets',
                    c_name=list(dataset.keys()),
                    value=list(dataset.values()),
                    row_num=idx + 1
                )
                matches.append(match)
        
        # Search in resources
        for idx, (resource_id, resource) in enumerate(self.resources.items()):
            if self._matches_condition(resource, column, operator, value):
                match = Value(
                    t_name='ndp_resources',
                    c_name=list(resource.keys()),
                    value=list(resource.values()),
                    row_num=idx + 1
                )
                matches.append(match)
        
        return matches if matches else None
    
    def find_table(self, query_object, kwargs):
        """Find tables matching name"""
        from dsi.core import Value
        
        matches = []
        for table_name in ['ndp_datasets', 'ndp_resources']:
            if query_object.lower() in table_name.lower():
                match = Value(
                    t_name=table_name,
                    c_name=None,
                    value=None,
                    row_num=None
                )
                matches.append(match)
        
        return matches if matches else None
    
    def find_column(self, query_object, kwargs):
        """Find columns matching name"""
        from dsi.core import Value
        
        matches = []
        
        # Check dataset columns
        if self.datasets:
            sample = next(iter(self.datasets.values()))
            for col in sample.keys():
                if query_object.lower() in col.lower():
                    match = Value(
                        t_name='ndp_datasets',
                        c_name=[col],
                        value=None,
                        row_num=None
                    )
                    matches.append(match)
        
        # Check resource columns
        if self.resources:
            sample = next(iter(self.resources.values()))
            for col in sample.keys():
                if query_object.lower() in col.lower():
                    match = Value(
                        t_name='ndp_resources',
                        c_name=[col],
                        value=None,
                        row_num=None
                    )
                    matches.append(match)
        
        return matches if matches else None
    
    def find_cell(self, query_object, kwargs):
        """Find cells containing value"""
        return self.find(f"* ~ {query_object}", kwargs)
    
    def _matches_condition(self, record, column, operator, value):
        """Check if record matches condition"""
        if column not in record:
            return False
        
        record_value = str(record[column]).lower()
        value = value.lower()
        
        if operator in ['=', '==']:
            return record_value == value
        elif operator == '!=':
            return record_value != value
        elif operator in ['~', '~~', 'LIKE']:
            return value in record_value
        elif operator == '>':
            try:
                return float(record_value) > float(value)
            except:
                return False
        elif operator == '<':
            try:
                return float(record_value) < float(value)
            except:
                return False
        elif operator == '>=':
            try:
                return float(record_value) >= float(value)
            except:
                return False
        elif operator == '<=':
            try:
                return float(record_value) <= float(value)
            except:
                return False
        
        return False
    
    def close(self):
        """Close the session"""
        self.session.close()
        self.datasets.clear()
        self.resources.clear()


# Alias for backward compatibility
CKAN = NDP
