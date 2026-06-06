from dsi.sync import Sync

# Origin location of data
remote_sources = "../federated/input.yaml"
# Remote Location where database and data will be moved
workspace = "dsi_data"

# Create Sync type with project database name
s = Sync("federated.db")
s.get(input_yaml=remote_sources,workspace_folder=workspace)

# Download data referenced in this database -- db must have been previously indexed by DSI
s.get_data("ocean_11_datasets.db", workspace)


from dsi.dsifederated import DSIFederated
federated_databases = DSIFederated(workspace, operating_mode="notebook")
federated_databases.f_display_databases()