from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={"keywords": "environmental", "limit": 25}
)

# Find datasets with more than 10 resources
results = dsi.find("num_resources > 3")