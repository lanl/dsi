from dsi.dsi import DSI

dsi = DSI(
    backend_name="NDP",
    params={
        "keywords": "plants",
        "limit": 2
    }
)

# dsi.summary()
dsi.display(table_name="datasets", display_cols=["id", "title", "num_resources"])
# dsi.display("datasets")
dsi.display("resources" , display_cols=["dataset_id", "resource_id", "resource_name"])