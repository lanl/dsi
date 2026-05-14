from dsi.sync import Sync
from dsi.dsifederated import DSIFederated

# Origin location of data
remote_sources = "../federated/input.yaml"
# Remote Location where database and data will be moved
workspace = "dsi_data"

# Create Sync type with project database name
s = Sync()
s.get(input_yaml=remote_sources,workspace_folder=workspace)

federated_databases = DSIFederated(workspace, operating_mode="notebook")
federated_databases.f_display_databases()