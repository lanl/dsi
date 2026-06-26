# examples/ndp/ndp_user/download_analyze.py
"""
Download and analyze PDF documentation from NDP deep water datasets.
"""

import os
import requests
from dsi.dsi import DSI

def main(verbose=False):
    """Download PDF documentation from NDP and extract metadata."""
    
    # Query NDP for deep water datasets
    print("\nQuerying NDP for deep water datasets...")
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "deep water", "limit": 10}
    )
    
    if verbose:
        print("\nAvailable tables:")
        dsi.list()
    
    # Query for PDF resources
    try:
        # Get all resources
        resources_df = dsi.resources
        print(f"\nFound {len(resources_df)} total resources")
        
        # Show format distribution
        if 'format' in resources_df.columns:
            formats = resources_df['format'].value_counts()
            print("\nResource formats:")
            for fmt, count in formats.items():
                print(f"  {fmt}: {count}")
        
        # Query for PDFs using DSI-style filtering
        pdf_query = resources_df.query("format.str.upper() == 'PDF'")
        
        if pdf_query.empty:
            print("\n❌ No PDF resources found")
            dsi.close()
            return
        
        print(f"\n✓ Found {len(pdf_query)} PDF(s)")
        
        # Get first PDF's info
        pdf_info = pdf_query.iloc[0]
        pdf_name = pdf_info['resource_name']
        pdf_url = pdf_info['url']
        dataset = pdf_info['dataset_title']
        
        print(f"  Name: {pdf_name}")
        print(f"  Dataset: {dataset}")
        
    except Exception as e:
        print(f"Error querying resources: {e}")
        dsi.close()
        return
    
    # Validate URL
    print(f"\nValidating URL: {pdf_url[:60]}...")
    
    try:
        response = requests.head(pdf_url, allow_redirects=True, timeout=10, verify=False)
        if response.status_code == 405:
            response = requests.get(pdf_url, stream=True, timeout=10, verify=False)
            response.close()
        
        if not (200 <= response.status_code < 400):
            print(f"URL not accessible (status: {response.status_code})")
            dsi.close()
            return
            
    except Exception as e:
        print(f"URL validation failed: {e}")
        dsi.close()
        return
    
    # Download PDF
    download_dir = "ndp_downloads"
    os.makedirs(download_dir, exist_ok=True)
    
    # Clean filename
    filename = pdf_name.replace('Documentation: ', '').strip()
    if not filename.lower().endswith('.pdf'):
        filename += '.pdf'
    
    filepath = os.path.join(download_dir, filename)
    print(f"\nDownloading to: {filepath}")
    
    try:
        response = requests.get(pdf_url, verify=False, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        file_size = os.path.getsize(filepath)
        print(f"Downloaded: {file_size:,} bytes")
        
    except Exception as e:
        print(f"Download failed: {e}")
        dsi.close()
        return
    
    # Extract PDF metadata
    try:
        from PyPDF2 import PdfReader
        
        reader = PdfReader(filepath)
        print(f"\nPDF Info:")
        print(f"  Pages: {len(reader.pages)}")
        
        if reader.metadata:
            metadata = reader.metadata
            if metadata.title:
                print(f"  Title: {metadata.title}")
            if metadata.author:
                print(f"  Author: {metadata.author}")
        
        if verbose and len(reader.pages) > 0:
            print(f"\nFirst page preview:")
            text = reader.pages[0].extract_text()
            print(text[:500] + "..." if len(text) > 500 else text)
    
    except ImportError:
        print("\nInstall PyPDF2 for metadata: pip install PyPDF2")
    except Exception as e:
        print(f"Could not read PDF: {e}")
    
    # Show related resources
    if verbose:
        print(f"\nAll resources in '{dataset}':")
        related = resources_df.query(f"dataset_title == '{dataset}'")
        for _, row in related.iterrows():
            print(f"  - {row['resource_name']} ({row['format']})")
        
        print(f"\nData summary:")
        dsi.summary()
    
    print(f"\n✓ Complete. File saved to: {filepath}")
    
    dsi.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Download PDF documentation from NDP deep water datasets"
    )
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed output")
    args = parser.parse_args()
    main(verbose=args.verbose)