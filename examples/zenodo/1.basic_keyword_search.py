"""
Example 1: Basic Zenodo keyword search.

This example shows the simplest Zenodo backend workflow:
- create backend
- search Zenodo using keywords
- list available DSI tables
- view datasets and resources
"""

import warnings

from urllib3.exceptions import InsecureRequestWarning

from dsi.backends.zenodo import Zenodo


warnings.simplefilter("ignore", InsecureRequestWarning)


def main():
    zenodo = Zenodo(
        params={
            "keywords": "climate",
            "limit": 3,
        },
        verify_ssl=False,
    )

    try:
        print("\nZenodo backend loaded successfully.")

        print("\nTable names:")
        print(zenodo.get_table_names())

        print("\nTable list:")
        zenodo.list()

        print("\nDatasets table:")
        datasets = zenodo.get_table("datasets")
        print(datasets.head())

        print("\nResources table:")
        resources = zenodo.get_table("resources")
        print(resources.head())

        print("\nShapes:")
        print("datasets:", datasets.shape)
        print("resources:", resources.shape)

    finally:
        zenodo.close()


if __name__ == "__main__":
    main()