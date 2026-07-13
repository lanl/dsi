from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={
        "license": "CC0-1.0",
        "limit": 30
    }
)

# dsi.summary()
dsi.display(table_name="datasets", display_cols=["title", "license"])
# dsi.display("datasets")
# dsi.display("resources")