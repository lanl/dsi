# # examples/ndp/ndp_user/1.load_basic.py
# from dsi.dsi import DSI

# def main(verbose=False):
#     # Initialize NDP backend with search parameters
#     dsi = DSI(
#         backend_name="NDP",
#         keywords="temperature",  # Search term
#         limit=5                  # Maximum datasets to retrieve
#     )
    
#     if verbose:
#         print("\nAvailable tables:")
#         dsi.list()
    
#     dsi.close()

# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--verbose", action="store_true")
#     args = parser.parse_args()
#     main(verbose=args.verbose)

# examples/ndp/ndp_user/1.search_by_id.py
# from dsi.dsi import DSI

# def main(verbose=False):
#     """
#     Search for a specific dataset by ID and print its details.
    
#     This example demonstrates how to:
#     1. Search for a dataset using its ID
#     2. Retrieve the datasets table
#     3. Display the dataset ID and key metadata
#     """
    
#     # Example dataset ID from NDP catalog
#     # Replace this with any valid dataset ID from nationaldataplatform.org
#     dataset_id = "5004ec53-2f10-4186-bda9-70866a0823f6"
    
#     print(f"Searching for dataset with ID: {dataset_id}")
    
#     # Initialize NDP backend with ID-based search
#     dsi = DSI(
#         backend_name="NDP",
#         params={
#             "id": dataset_id,  # Search by ID using Solr field syntax
#             "limit": 1
#         }
#     )
    
#     # Get the datasets table
#     datasets_df = dsi.get_table("datasets", collection=True)
    
#     if datasets_df.empty:
#         print(f"\nNo dataset found with ID: {dataset_id}")
#     else:
#         print("\n" + "="*60)
#         print("DATASET FOUND")
#         print("="*60)
        
#         # Print key dataset information
#         dataset = datasets_df.iloc[0]
#         print(f"\nDataset ID:      {dataset['id']}")
#         print(f"Name:            {dataset['name']}")
#         print(f"Title:           {dataset['title']}")
#         print(f"Organization:    {dataset['organization']}")
#         print(f"Num Resources:   {dataset['num_resources']}")
#         print(f"Created:         {dataset['created']}")
#         print(f"Modified:        {dataset['modified']}")
        
#         if verbose:
#             print("\n" + "-"*60)
#             print("FULL DATASET METADATA")
#             print("-"*60)
#             for col in datasets_df.columns:
#                 if col != 'raw_dataset':  # Skip raw data for cleaner output
#                     print(f"\n{col}:")
#                     print(f"  {dataset[col]}")
            
#             # Show resources if any
#             if dataset['num_resources'] > 0:
#                 resources_df = dsi.get_table("resources", collection=True)
#                 print("\n" + "-"*60)
#                 print("ASSOCIATED RESOURCES")
#                 print("-"*60)
#                 for idx, resource in resources_df.iterrows():
#                     print(f"\nResource {idx + 1}:")
#                     print(f"  Name:   {resource['resource_name']}")
#                     print(f"  Format: {resource['format']}")
#                     print(f"  URL:    {resource['url']}")
    
#     dsi.close()

# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--verbose", action="store_true", 
#                        help="Show full dataset and resource details")
#     parser.add_argument("--id", type=str, 
#                        help="Dataset ID to search for (default: adur-district-spending)")
#     args = parser.parse_args()
    
#     # Allow custom ID via command line
#     if args.id:
#         # Modify the main function to accept ID parameter
#         import sys
#         # Simple workaround: replace the dataset_id in main
#         main_code = main.__code__
#         main_globals = {'DSI': DSI, 'dataset_id': args.id}
#         exec(main_code, main_globals)
#     else:
#         main(verbose=args.verbose)

from dsi.dsi import DSI

dsi = DSI(backend_name="NDP", params={"keywords":"temp", "limit":5})

df = dsi.get_table("datasets", collection=True)

# View ALL CKAN fields for first dataset
import json
print(json.dumps(df.iloc[0]['raw_dataset'], indent=2))

dsi.close()