from dsi.sync import Sync
from dsi.utils.dsi_utils import f_dsi

# Origin location of data
remote_sources = "../federated/input.yaml"
# Remote Location where database and data will be moved
workspace = "dsi_data"

# Create Sync type with project database name
s = Sync()
s.get(remote_sources,workspace)

federated_databases = f_dsi(workspace, operating_mode="notebook")
federated_databases.f_display_databases()