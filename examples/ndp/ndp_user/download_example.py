# examples/ndp/ndp_user/download_analyze.py
import os
import requests
from dsi.dsi import DSI

def main(verbose=False):
    """
    Demonstrates a complete scientific workflow for downloading documentation:
    1. Query Oceans11 for wildfire simulation dataset
    2. Locate PDF documentation resource
    3. Validate URL before downloading
    4. Download the PDF file
    5. Display PDF metadata
    
    This workflow is useful for retrieving scientific documentation,
    technical reports, and supplementary materials from NDP datasets.
    """
    
    # Step 1: Query Oceans11 organization for wildfire datasets
    print("\n" + "="*70)
    print("STEP 1: Query Oceans11 - LANL for wildfire simulation datasets")
    print("="*70)
    
    dsi = DSI(
        backend_name="NDP",
        keywords="wildfire oceans11",  # Look for wildfire datasets
        limit=10
    )
    
    datasets_df = dsi.get_table("datasets", collection=True)
    
    if datasets_df.empty:
        print("No datasets found for this organization")
        dsi.close()
        return
    
    print(f"\nFound {len(datasets_df)} dataset(s) from Oceans11 - LANL")
    print("\nDataset titles:")
    for idx, row in datasets_df.iterrows():
        print(f"  {idx+1}. {row['title']}")
        print(f"     Author: {row['author']}")
        print(f"     Resources: {row['num_resources']}")
    
    # Step 2: Explore resource files and find PDF documentation
    print("\n" + "="*70)
    print("STEP 2: Locate PDF documentation")
    print("="*70)
    
    table_list = dsi.list(collection=True)
    resource_tables = [t for t in table_list if t != 'datasets']
    
    if not resource_tables:
        print("No resource files found")
        dsi.close()
        return
    
    print(f"\nFound {len(resource_tables)} dataset(s) with resources")
    
    # Look for PDF documentation
    selected_resource_table = None
    selected_resource = None
    
    for table_name in resource_tables:
        resource_df = dsi.get_table(table_name, collection=True)
        
        # Filter by PDF format
        pdf_resources = resource_df[
            resource_df['format'].str.upper() == 'PDF'
        ]
        
        if not pdf_resources.empty:
            selected_resource_table = table_name
            selected_resource = pdf_resources.iloc[0]
            break
    
    if selected_resource is None:
        print("\nNo PDF resources found")
        print("\nAvailable formats in first dataset:")
        first_resources = dsi.get_table(resource_tables[0], collection=True)
        print(first_resources['format'].unique())
        dsi.close()
        return
    
    print(f"\nSelected dataset: {selected_resource_table}")
    print(f"Selected resource: {selected_resource['resource_name']}")
    print(f"Format: {selected_resource['format']}")
    print(f"URL: {selected_resource['url']}")
    
    # Step 3: Validate URL before downloading
    print("\n" + "="*70)
    print("STEP 3: Validate resource URL")
    print("="*70)
    
    url = selected_resource['url']
    
    try:
        print(f"\nValidating URL: {url}")
        
        # HEAD request to check if URL is accessible
        response = requests.head(
            url,
            allow_redirects=True,
            timeout=10,
            verify=False  # Disable SSL verification for testing
        )
        
        # Some servers don't support HEAD, try GET with stream
        if response.status_code == 405:
            response = requests.get(
                url,
                stream=True,
                timeout=10,
                verify=False
            )
            response.close()
        
        if 200 <= response.status_code < 400:
            print(f"✓ URL is valid (Status: {response.status_code})")
            
            # Display content type if available
            content_type = response.headers.get('Content-Type', 'Unknown')
            print(f"  Content-Type: {content_type}")
            
            # Display file size if available
            content_length = response.headers.get('Content-Length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                print(f"  File Size: {size_mb:.2f} MB ({content_length} bytes)")
            
            url_valid = True
        else:
            print(f"✗ URL returned status code: {response.status_code}")
            url_valid = False
            
    except requests.exceptions.Timeout:
        print("✗ URL validation timed out")
        url_valid = False
    except requests.exceptions.ConnectionError:
        print("✗ Connection error - URL may be unreachable")
        url_valid = False
    except Exception as e:
        print(f"✗ Error validating URL: {e}")
        url_valid = False
    
    if not url_valid:
        print("\nCannot proceed - URL is not accessible")
        dsi.close()
        return
    
    # Step 4: Download the PDF file
    print("\n" + "="*70)
    print("STEP 4: Download the PDF documentation")
    print("="*70)
    
    # Create downloads directory
    download_dir = "ndp_downloads"
    os.makedirs(download_dir, exist_ok=True)
    
    # Sanitize filename
    filename = selected_resource['resource_name']
    if not filename:
        filename = f"resource_{selected_resource['resource_id']}.pdf"
    elif not filename.lower().endswith('.pdf'):
        filename += '.pdf'
    
    filepath = os.path.join(download_dir, filename)
    
    try:
        print(f"\nDownloading to: {filepath}")
        
        response = requests.get(url, verify=False, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        file_size = os.path.getsize(filepath)
        print(f"✓ Download complete ({file_size:,} bytes)")
        
    except Exception as e:
        print(f"✗ Download failed: {e}")
        dsi.close()
        return
    
    # Step 5: Display PDF metadata
    print("\n" + "="*70)
    print("STEP 5: Display PDF metadata")
    print("="*70)
    
    try:
        # Try to import PyPDF2 for metadata extraction
        try:
            from PyPDF2 import PdfReader
            
            reader = PdfReader(filepath)
            
            print(f"\n✓ Successfully opened PDF: {filename}")
            print(f"\nPDF Metadata:")
            print(f"  Number of pages: {len(reader.pages)}")
            
            # Extract metadata if available
            if reader.metadata:
                metadata = reader.metadata
                print(f"\nDocument Information:")
                if metadata.title:
                    print(f"  Title: {metadata.title}")
                if metadata.author:
                    print(f"  Author: {metadata.author}")
                if metadata.subject:
                    print(f"  Subject: {metadata.subject}")
                if metadata.creator:
                    print(f"  Creator: {metadata.creator}")
                if metadata.producer:
                    print(f"  Producer: {metadata.producer}")
                if metadata.creation_date:
                    print(f"  Created: {metadata.creation_date}")
            
            # Extract text from first page if verbose
            if verbose and len(reader.pages) > 0:
                print(f"\nFirst page text preview:")
                print("-" * 60)
                first_page = reader.pages[0]
                text = first_page.extract_text()
                # Print first 500 characters
                preview = text[:500] if len(text) > 500 else text
                print(preview)
                if len(text) > 500:
                    print("\n[... truncated ...]")
                print("-" * 60)
        
        except ImportError:
            print("\nNote: Install PyPDF2 for PDF metadata extraction:")
            print("  pip install PyPDF2")
            print(f"\nPDF downloaded successfully to: {filepath}")
            print(f"File size: {file_size:,} bytes")
            print(f"\nYou can open this file with any PDF reader.")
    
    except Exception as e:
        print(f"Note: Could not extract PDF metadata: {e}")
        print(f"\nPDF downloaded successfully to: {filepath}")
    
    # Step 6: Display dataset information
    print("\n" + "="*70)
    print("STEP 6: Dataset information")
    print("="*70)
    
    # Get the dataset row
    dataset_row = datasets_df[datasets_df['title'] == selected_resource_table].iloc[0]
    
    print(f"\nDataset: {dataset_row['title']}")
    print(f"Organization: {dataset_row['organization']}")
    print(f"Author: {dataset_row['author']}")
    print(f"Created: {dataset_row['created']}")
    
    if dataset_row['notes']:
        print(f"\nDescription:")
        print("-" * 60)
        # Print first 300 characters of description
        description = dataset_row['notes']
        if len(description) > 300:
            print(description[:300] + "...")
        else:
            print(description)
        print("-" * 60)
    
    if verbose:
        # List all resources for this dataset
        print(f"\n\nAll resources for this dataset:")
        all_resources = dsi.get_table(selected_resource_table, collection=True)
        for idx, row in all_resources.iterrows():
            print(f"\n  {idx+1}. {row['resource_name']}")
            print(f"     Format: {row['format']}")
            print(f"     URL: {row['url']}")
    
    # Summary
    print("\n" + "="*70)
    print("WORKFLOW COMPLETE")
    print("="*70)
    print(f"\nDataset: {selected_resource_table}")
    print(f"Documentation: {filename}")
    print(f"Downloaded to: {filepath}")
    print(f"File size: {file_size:,} bytes")
    print(f"\nAll outputs saved to: {download_dir}/")
    print("\nNext steps:")
    print("  1. Open the PDF to review the documentation")
    print("  2. Explore other resources in this dataset")
    print("  3. Download data files for analysis")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Download PDF documentation from Oceans11 dataset"
    )
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed output including text previews")
    args = parser.parse_args()
    main(verbose=args.verbose)