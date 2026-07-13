from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={
        "keywords": "science",
        "group": "data_hub_cc_wstc",
        "limit": 10
    }
)

dsi.summary()
dsi.display(table_name="datasets", display_cols=["name", "group", "license"])