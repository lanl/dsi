# examples/ndp/ndp_user/1.ingest.py
from dsi.dsi import DSI

def main():
    # Initialize DSI (no file needed for web backend, but required arg)
    dsi = DSI("ndp.db")

    # Read from NDP backend
    dsi.read(
        "",
        "NDP",
        kwargs={
            "keywords": "data",
            "limit": 5
        }
    )

    # Show tables
    dsi.list()

    # Display datasets table
    dsi.display("datasets")

    dsi.close()

if __name__ == "__main__":
    main()