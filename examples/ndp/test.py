from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={"keywords": "water quality", "limit": 20}
)

dsi.list()

print(dsi.schema())