from dsi.dsi import DSI

# Query from NDP
dsi = DSI(
    backend_name="NDP",
    params={"keywords": "climate", "limit": 10}
)

# Process NDP data to local SQLite database
dsi.process(
    backend_name="Sqlite",
    filename="climate_data.db"
)
dsi.summary()
dsi.close()

# Load the newly created database
local_dsi = DSI(
    backend_name="Sqlite",
    filename="climate_data.db"
)

# Query the local database
datasets = local_dsi.get_table("datasets", collection=True)
print(f"Loaded {len(datasets)} datasets from local database")
local_dsi.summary()
local_dsi.close()

# # dsi = DSI(
# #     backend_name="NDP",
# #     params={"group": "data_hub_cc_wstc", "limit": 10}
# # )

# dsi = DSI(
#     backend_name="NDP",
#     params={"keywords": "space", "limit": 10}
# )

# dsi.list()
# dsi.summary()
# dsi.display(table_name="datasets", display_cols=["name", "group"])