# examples/maglab/3.multiple_nodes.py
"""
Fetch and merge multiple OSF nodes into one set of Maglab tables.
"""

from dsi.dsi import DSI

def main():
    # Pass a list of params dicts to merge multiple OSF nodes into one
    # datasets/files/relationships table set
    dsi = DSI(
        backend_name="Maglab",
        params=[
            {"node_id": "8r2b3"},
            {"node_id": "gvudy"}
        ]
    )

    print("\nTable List (combined from both nodes):")
    dsi.list()

    datasets_df = dsi.get_table("datasets", collection=True)
    print("\nDatasets loaded:")
    print(datasets_df[["node_id", "title"]])

    dsi.close()

if __name__ == "__main__":
    main()
