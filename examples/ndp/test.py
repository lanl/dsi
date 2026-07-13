from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={
        "keywords": "climate",
        "organization": "California Landscape Metrics",
        "tags": ["climate refugia"],
        "formats": ["GeoTiff"],
        "limit": 25
    }
)

dsi.summary()