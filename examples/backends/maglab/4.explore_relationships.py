# examples/maglab/4.explore_relationships.py
"""
Explore the relationships table to see how a dataset fits into the
broader OSF project structure.
"""

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": "8r2b3"}
    )

    # The relationships table has one row per relationship pointer on the
    # node (e.g. children, parent, root, contributors), excluding 'files'
    # since that relationship is already expanded into the files table.
    relationships_df = dsi.get_table("relationships", collection=True)

    print("\nAll relationship pointers:")
    print(relationships_df[["relationship_name", "href"]])

    # Checking for 'parent' or 'root' pointers tells you whether this
    # dataset is a sub-node of a larger OSF project.
    print("\nLooking for 'parent' and 'root' pointers:")
    structural = relationships_df[
        relationships_df["relationship_name"].isin(["parent", "root", "children"])
    ]
    print(structural[["relationship_name", "href"]])

    dsi.close()

if __name__ == "__main__":
    main()
