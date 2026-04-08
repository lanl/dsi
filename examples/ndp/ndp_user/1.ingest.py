# examples/ndp/ndp_user/1.ingest.py
from dsi.dsi import DSI

def main():
    # Initialize DSI (no file needed for web backend, but required arg)
    dsi = DSI("ndp.db", webargs={
            "keywords": "data",
            "limit": 5
        })

    # Show tables
    dsi.list()

    # Display datasets table
    dsi.display("datasets")

    # Disable Read (but no error)
    dsi.query("`num_resources` > 10", {"table": "datasets", "dict_return": True})

    dsi.close()

if __name__ == "__main__":
    main()