# examples/ndp/1.load_basic.py
"""
Basic NDP search using keywords.
"""

from dsi.dsi import DSI

def main():
    # Initialize NDP backend with basic keyword search
    dsi = DSI(
        backend_name="NDP",
        params={"keywords": "climate", "limit": 10}
    )
    
    print("\nTable List:")
    dsi.list()
    
    print("\nTable Summary:")
    dsi.summary()
    
    dsi.close()

if __name__ == "__main__":
    main()