from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={"group": "data_hub_cc_wstc", "limit": 10}
)

dsi.list()
dsi.summary()
dsi.display(table_name="datasets", display_cols=["name", "group"])