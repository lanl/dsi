# examples/maglab/1.load_basic.py
"""
Basic Maglab load using a single OSF node id.
"""

from dsi.dsi import DSI

def main():
    # Initialize Maglab backend with a single OSF node
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": "8r2b3"}
    )

    print("\nTable List:")
    dsi.list()

    print("\nTable Summary:")
    dsi.summary()

    dsi.close()

if __name__ == "__main__":
    main()
