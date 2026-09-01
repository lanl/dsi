from dsi.sync import Sync

# Origin location of data
remote_sources = "../federated/input.yaml"
# Remote Location where database and data will be moved
workspace = "dsi_data"

# Create Sync type with project database name
s = Sync("federated.db")
s.get(config_file=remote_sources, workspace_folder=workspace)

# Download data referenced in this database -- db must have been previously indexed by DSI
s.get_data(db_name="oceans11.db", workspace_folder=workspace)


from dsi.dsifederated import DSIFederated
federated_databases = DSIFederated(workspace, operating_mode="notebook")
federated_databases.f_list_databases()