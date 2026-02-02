#!/usr/bin/env python3
"""
CKAN Discovery Module
Handles all CKAN API operations: search, browse, download, export

Usage:
    from ckan_discovery import CKANClient
    
    client = CKANClient()
    datasets = client.search_datasets(keywords="climate")
    client.export_to_csv(datasets, "results.csv")

CLI Examples:
    # Basic search with verbose output
    python ckan_discovery.py --search "climate" --verbose --limit 5

    # Search and export to CSV
    python ckan_discovery.py --search "climate" --formats CSV --export-csv results.csv

    # Search and export to JSON (complete metadata)
    python ckan_discovery.py --search "climate" --export-json results.json

    # Export only datasets (skip resources CSV)
    python ckan_discovery.py --search "climate" --export-csv results.csv --no-resources

    # Detailed view (more than basic, less than verbose)
    python ckan_discovery.py --search "climate" --detailed

    # Complete verbose JSON output
    python ckan_discovery.py --search "climate" --formats CSV --verbose --limit 1

    # Combine search with export
    python ckan_discovery.py --organization lanl --formats CSV JSON --export-csv lanl_data.csv --export-json lanl_data.json
"""

import requests
import json
import os
import csv
import warnings
import certifi
from typing import List, Dict, Optional
from datetime import datetime


class CKANClient:
    """Client for interacting with CKAN API"""
        
    def __init__(self, base_url="https://nationaldataplatform.org/catalog",
                 api_key=None, verify_ssl=False):
        """
        Initialize CKAN client
        
        Args:
            base_url: CKAN instance base URL
            api_key: API key for authenticated requests
            verify_ssl: Whether to verify SSL certificates (default: False)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.verify_ssl = verify_ssl
        self.headers = {'Authorization': api_key} if api_key else {}

    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """
        Make API request to CKAN
        
        Args:
            endpoint: API endpoint (e.g., 'package_search')
            params: Query parameters
            
        Returns:
            dict: JSON response or None on error
        """
        url = f"{self.base_url}/api/3/action/{endpoint}"

        try:
            if self.verify_ssl:
                response = requests.get(url, params=params, headers=self.headers, 
                                      verify=certifi.where())
            else:
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

    # ========================================================================
    #                          SEARCH & DISCOVERY
    # ========================================================================
    
    def search_datasets(self, keywords: str = None, organization: str = None, 
                       tags: List[str] = None, formats: List[str] = None, 
                       limit: int = 10) -> List[Dict]:
        """
        Search for datasets in CKAN catalog
        
        Args:
            keywords: Search keywords
            organization: Filter by organization name
            tags: Filter by tags
            formats: Filter by resource formats
            limit: Maximum results to return
            
        Returns:
            list: List of dataset dictionaries
        """
        params = {'rows': limit}
        q_parts = []
        fq_parts = []

        # Build query
        if keywords:
            q_parts.append(keywords)
        
        if organization:
            # Handle common aliases
            if organization.lower() in ['lanl', 'los-alamos']:
                organization = "los-alamos-national-laboratory"
            fq_parts.append(f'organization:{organization}')

        if tags:
            for tag in tags:
                fq_parts.append(f'tags:{tag}')

        if formats:
            format_queries = [f'res_format:{fmt}' for fmt in formats]
            fq_parts.append('(' + ' OR '.join(format_queries) + ')')

        # Construct params
        if q_parts:
            params['q'] = ' '.join(q_parts)
        if fq_parts:
            params['fq'] = ' AND '.join(fq_parts)
        
        result = self._make_request('package_search', params)

        if result:
            total_count = result.get('count', 0)
            datasets = result.get('results', [])
            
            # Build search summary
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

    def get_dataset(self, dataset_id: str, verbose: bool = False) -> Optional[Dict]:
        """
        Get detailed information about a specific dataset
        
        Args:
            dataset_id: Dataset ID or name
            verbose: If True, print complete JSON
            
        Returns:
            dict: Dataset information or None
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
        Get detailed information about a specific resource
        
        Args:
            resource_id: Resource ID
            verbose: If True, print complete JSON
            
        Returns:
            dict: Resource information or None
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

    def list_organizations(self) -> List[Dict]:
        """
        List all organizations in CKAN
        
        Returns:
            list: List of organization dictionaries
        """
        result = self._make_request('organization_list', {'all_fields': True})
        
        if result:
            print(f"✓ Found {len(result)} organizations")
            return result
        
        return []

    def get_site_statistics(self) -> Dict:
        """
        Get CKAN site statistics
        
        Returns:
            dict: Statistics (dataset count, org count, etc.)
        """
        # Get dataset count
        search_result = self._make_request('package_search', {'rows': 0})
        dataset_count = search_result.get('count', 0) if search_result else 0
        
        # Get organization count
        org_result = self._make_request('organization_list')
        org_count = len(org_result) if org_result else 0
        
        # Get group count
        group_result = self._make_request('group_list')
        group_count = len(group_result) if group_result else 0
        
        stats = {
            'datasets': dataset_count,
            'organizations': org_count,
            'groups': group_count
        }
        
        return stats

    # ========================================================================
    #                          DOWNLOAD OPERATIONS
    # ========================================================================

    def download_resource(self, resource_id: str, output_dir: str = "./downloads",
                         filename: str = None) -> Optional[str]:
        """
        Download a resource file
        
        Args:
            resource_id: Resource ID to download
            output_dir: Directory to save file
            filename: Optional custom filename
            
        Returns:
            str: Path to downloaded file or None on error
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
            # Add extension if missing
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
        
        Args:
            dataset_id: Dataset ID or name
            output_dir: Directory to save files
            formats: Optional list of formats to filter (e.g., ['csv', 'json'])
            
        Returns:
            list: List of downloaded file paths
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

    # ========================================================================
    #                          EXPORT OPERATIONS
    # ========================================================================

    def extract_metadata(self, datasets: List[Dict]) -> Dict:
        """
        Extract structured metadata from CKAN datasets for loading into DSI tables.
        
        Args:
            datasets: List of dataset dictionaries from CKAN API
            
        Returns:
            dict: Dictionary with 'datasets' and 'resources' lists ready for 
                  DataFrame conversion
        """
        datasets_data = []
        resources_data = []
        
        for dataset in datasets:
            # Extract dataset-level metadata
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
            
            # Extract resource-level metadata
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

    def export_to_csv(self, datasets: List[Dict], output_file: str, 
                     include_resources: bool = True) -> bool:
        """
        Export dataset metadata to CSV
        
        Args:
            datasets: List of dataset dictionaries
            output_file: Output CSV filename
            include_resources: If True, create separate resource CSV
            
        Returns:
            bool: True if successful
        """
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            
            # Export datasets
            dataset_fields = [
                'id', 'name', 'title', 'notes', 'url',
                'organization_name', 'organization_title',
                'author', 'author_email', 'maintainer', 'maintainer_email',
                'license_id', 'license_title',
                'metadata_created', 'metadata_modified',
                'tags', 'num_resources', 'num_tags',
                'type', 'state', 'private', 'isopen'
            ]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=dataset_fields, 
                                       extrasaction='ignore')
                writer.writeheader()
                
                for dataset in datasets:
                    row = {
                        'id': dataset.get('id', ''),
                        'name': dataset.get('name', ''),
                        'title': dataset.get('title', ''),
                        'notes': dataset.get('notes', ''),
                        'url': dataset.get('url', ''),
                        'organization_name': dataset.get('organization', {}).get('name', ''),
                        'organization_title': dataset.get('organization', {}).get('title', ''),
                        'author': dataset.get('author', ''),
                        'author_email': dataset.get('author_email', ''),
                        'maintainer': dataset.get('maintainer', ''),
                        'maintainer_email': dataset.get('maintainer_email', ''),
                        'license_id': dataset.get('license_id', ''),
                        'license_title': dataset.get('license_title', ''),
                        'metadata_created': dataset.get('metadata_created', ''),
                        'metadata_modified': dataset.get('metadata_modified', ''),
                        'tags': ', '.join(tag['name'] for tag in dataset.get('tags', [])),
                        'num_resources': dataset.get('num_resources', 
                                                    len(dataset.get('resources', []))),
                        'num_tags': dataset.get('num_tags', len(dataset.get('tags', []))),
                        'type': dataset.get('type', ''),
                        'state': dataset.get('state', ''),
                        'private': dataset.get('private', ''),
                        'isopen': dataset.get('isopen', '')
                    }
                    writer.writerow(row)
            
            print(f"✓ Exported {len(datasets)} datasets to: {output_file}")
            
            # Export resources if requested
            if include_resources:
                resource_file = output_file.replace('.csv', '_resources.csv')
                self._export_resources_to_csv(datasets, resource_file)
            
            return True
            
        except Exception as e:
            print(f"✗ Export failed: {str(e)}")
            return False

    def _export_resources_to_csv(self, datasets: List[Dict], output_file: str) -> bool:
        """Export resource metadata to CSV"""
        try:
            resource_fields = [
                'resource_id', 'resource_name', 'format', 'mimetype',
                'size', 'url', 'description',
                'created', 'last_modified', 'hash',
                'dataset_id', 'dataset_name', 'dataset_title',
                'state', 'datastore_active'
            ]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=resource_fields, 
                                       extrasaction='ignore')
                writer.writeheader()
                
                resource_count = 0
                for dataset in datasets:
                    for resource in dataset.get('resources', []):
                        row = {
                            'resource_id': resource.get('id', ''),
                            'resource_name': resource.get('name', ''),
                            'format': resource.get('format', ''),
                            'mimetype': resource.get('mimetype', ''),
                            'size': resource.get('size', ''),
                            'url': resource.get('url', ''),
                            'description': resource.get('description', ''),
                            'created': resource.get('created', ''),
                            'last_modified': resource.get('last_modified', ''),
                            'hash': resource.get('hash', ''),
                            'dataset_id': dataset.get('id', ''),
                            'dataset_name': dataset.get('name', ''),
                            'dataset_title': dataset.get('title', ''),
                            'state': resource.get('state', ''),
                            'datastore_active': resource.get('datastore_active', '')
                        }
                        writer.writerow(row)
                        resource_count += 1
            
            print(f"✓ Exported {resource_count} resources to: {output_file}")
            return True
            
        except Exception as e:
            print(f"✗ Resource export failed: {str(e)}")
            return False

    def export_to_json(self, datasets: List[Dict], output_file: str, 
                      pretty: bool = True) -> bool:
        """
        Export complete dataset metadata to JSON
        
        Args:
            datasets: List of dataset dictionaries
            output_file: Output JSON filename
            pretty: If True, format with indentation
            
        Returns:
            bool: True if successful
        """
        try:
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            
            export_data = {
                'export_date': datetime.now().isoformat(),
                'dataset_count': len(datasets),
                'datasets': datasets
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(export_data, f, ensure_ascii=False)
            
            print(f"✓ Exported {len(datasets)} datasets to JSON: {output_file}")
            return True
            
        except Exception as e:
            print(f"✗ JSON export failed: {str(e)}")
            return False

    # ========================================================================
    #                          DISPLAY HELPERS
    # ========================================================================

    def display_datasets(self, datasets: List[Dict], detailed: bool = False, 
                        verbose: bool = False):
        """
        Display dataset information in readable format
        
        Args:
            datasets: List of dataset dictionaries
            detailed: Show detailed information
            verbose: Show complete JSON for each dataset
        """
        if not datasets:
            print("\nNo datasets to display.")
            return
        
        for i, dataset in enumerate(datasets, 1):
            print(f"\n{'='*70}")
            print(f"--- Dataset: {dataset.get('name', 'unnamed')} ---")
            print(f"{'='*70}")
            
            if verbose:
                # Print complete JSON
                print(json.dumps(dataset, indent=2))
            else:
                # Print formatted output
                print(f"Title: {dataset.get('title', 'N/A')}")
                
                if dataset.get('notes'):
                    notes = dataset['notes']
                    if len(notes) > 300:
                        notes = notes[:300] + '...'
                    print(f"Description: {notes}")
                
                print(f"Organization: {dataset.get('organization', {}).get('title', 'N/A')}")
                
                resources = dataset.get('resources', [])
                print(f"Resources: {len(resources)}")
                
                if detailed and resources:
                    print("List of files: ")
                    for res in resources:
                        print(f"- {res.get('name', 'Unnamed')} ({res.get('format', 'Unknown format')})")
                        if res.get('description'):
                            print(f"  Description: {res['description']}")
                        print(f"  URL: {res.get('url', 'N/A')}")
                        print(f"  Size: {res.get('size', 'None')}")
                        print(f"  ID: {res.get('id', 'N/A')}")

    def display_statistics(self):
        """Display CKAN site statistics"""
        stats = self.get_site_statistics()
        
        print(f"\n{'='*60}")
        print("CKAN SITE STATISTICS")
        print(f"{'='*60}")
        print(f"Total Datasets: {stats['datasets']}")
        print(f"Organizations: {stats['organizations']}")
        print(f"Groups: {stats['groups']}")
        print()


def main():
    """Example usage of CKAN Discovery module"""
    import argparse

    parser = argparse.ArgumentParser(description="CKAN Discovery Tool")
    
    # Search options
    parser.add_argument("--search", help="Search keywords")
    parser.add_argument("--organization", help="Filter by organization")
    parser.add_argument("--formats", nargs='+', help="Filter by formats (e.g., CSV JSON)")
    parser.add_argument("--limit", type=int, default=10, help="Maximum results (default: 10)")
    
    # Download options
    parser.add_argument("--download", help="Download resource by ID")
    parser.add_argument("--download-dataset", help="Download all resources from dataset")
    parser.add_argument("--output-dir", default="./downloads", help="Download directory")
    
    # Export options
    parser.add_argument("--export-csv", help="Export results to CSV file")
    parser.add_argument("--export-json", help="Export results to JSON file")
    parser.add_argument("--no-resources", action="store_true", 
                       help="Don't export resources CSV (only with --export-csv)")
    
    # Display options
    parser.add_argument("--verbose", action="store_true", 
                       help="Show complete JSON metadata")
    parser.add_argument("--detailed", action="store_true",
                       help="Show detailed information (between normal and verbose)")
    parser.add_argument("--stats", action="store_true", help="Show site statistics")
    
    # Connection options
    parser.add_argument("--no-verify-ssl", action="store_true", 
                       help="Disable SSL verification")

    args = parser.parse_args()

    if args.no_verify_ssl:
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    # Initialize client
    client = CKANClient(verify_ssl=not args.no_verify_ssl)

    # Execute actions
    if args.stats:
        client.display_statistics()

    # Search and display
    if args.search or args.organization or args.formats:
        datasets = client.search_datasets(
            keywords=args.search,
            organization=args.organization,
            formats=args.formats,
            limit=args.limit
        )
        
        if datasets:
            # Display results
            client.display_datasets(datasets, detailed=args.detailed, verbose=args.verbose)
            
            # Export if requested
            if args.export_csv:
                client.export_to_csv(datasets, args.export_csv, 
                                    include_resources=not args.no_resources)
            
            if args.export_json:
                client.export_to_json(datasets, args.export_json)
        else:
            print("\nNo datasets found matching criteria.")

    # Download operations
    if args.download:
        client.download_resource(args.download, output_dir=args.output_dir)

    if args.download_dataset:
        client.download_dataset_resources(
            args.download_dataset,
            output_dir=args.output_dir,
            formats=args.formats
        )


if __name__ == "__main__":
    main()
