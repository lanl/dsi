# examples/ndp/2.load_advanced.py
"""
Advanced NDP search with multiple filter parameters.
"""

from dsi.dsi import DSI

def main():
    # Initialize NDP backend with advanced search parameters
    dsi = DSI(
        backend_name="NDP",
        params={
            "keywords": "tree state park",
            "organization": "BurnPro3D",
            "group": "data_hub_cc_wstc",
            "tags": ["boundaries", "burn-units"],
            "formats": ["GeoJSON"],
            "limit": 15
        }
    )
    
    print("\nTable List:")
    dsi.list()
    
    print("\nTable Summary:")
    dsi.summary()
    
    # Display resources table (shows all columns and content)
    dsi.display("resources")
    
    dsi.close()

if __name__ == "__main__":
    main()