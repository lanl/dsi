# DSI Federated Data Acquisition

Core utilities for federated data acquisition across multiple HPC clusters with jump host and Kerberos support.

## Architecture

This package contains reusable core functions that can be used by:
- Web UI (`tools/federated/app_inter.py`)
- CLI tools
- Jupyter notebooks
- Other Python scripts

## Modules

### `discovery.py`
Endpoint discovery with jump host support.

**Main Function:** `discover_endpoints_async()`

Discovers DSI endpoints from remote HPC systems. Supports:
- Direct SSH connection
- Jump host with Kerberos authentication (reticket pattern)

**Example:**
```python
from dsi.utils.federated import discover_endpoints_async
import asyncio

endpoints = asyncio.run(discover_endpoints_async(
    hostname="tuolumne.llnl.gov",
    username="grosset2",
    script_path="/g/g92/grosset2/dsi_test/load_dsi_endpoints.sh",
    prefixes=['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'],
    hpc_type='kerberos',
    jump_host="ro-rfe.lanl.gov",
    jump_username="pascalgrosset",
    jump_password="..."
))
# Returns: {'DSI_ENDPOINT_LLNL1': '/g/g92/grosset2/dsi_test/db_source.csv', ...}
```

### `download.py`
File download utilities with jump host support.

**Main Functions:** 
- `download_csv_files_async()` - Download multiple CSV files
- `download_database_file_async()` - Download a single database file

Both support:
- Direct SSH connection  
- Jump host with Kerberos authentication (reticket pattern)
- Two-method fallback (asyncssh GSSAPI + SCP command)

**Example (CSV files):**
```python
from dsi.utils.federated import download_csv_files_async
import asyncio

files = asyncio.run(download_csv_files_async(
    hostname="tuolumne.llnl.gov",
    csv_paths={'DSI_ENDPOINT_LLNL1': '/g/g92/grosset2/dsi_test/db_source.csv'},
    temp_folder="/tmp/csv_downloads",
    username="grosset2",
    jump_host="ro-rfe.lanl.gov",
    jump_username="pascalgrosset",
    jump_password="...",
    jump_host_required=True
))
# Returns: ['/tmp/csv_downloads/tuolumne.llnl.gov_DSI_ENDPOINT_LLNL1.csv']
```

**Example (Database file):**
```python
from dsi.utils.federated import download_database_file_async
import asyncio

file_path = asyncio.run(download_database_file_async(
    hostname="tuolumne.llnl.gov",
    remote_path="/g/g92/grosset2/dsi_test/simplefolks.sqlite",
    local_folder="/tmp/databases",
    username="grosset2",
    jump_host="ro-rfe.lanl.gov",
    jump_username="pascalgrosset",
    jump_password="...",
    jump_host_required=True
))
# Returns: '/tmp/databases/simplefolks.sqlite'
```

## Jump Host Authentication Pattern

For systems requiring Kerberos authentication via a jump host:

1. **SSH to jump host** with username/password
2. **Run `reticket`** on jump host to obtain Kerberos tickets
3. **SSH from jump host to final HPC** using Kerberos (GSSAPI)

This pattern is automatically handled when `jump_host_required=True`.

### Two-Method Fallback

If GSSAPI authentication fails, the functions automatically fall back to running SSH/SCP commands directly on the jump host:

- **Method 1:** asyncssh with `gss_auth=True` (preferred)
- **Method 2:** Execute `ssh` or `scp` command on jump host (fallback)

## Integration with Web UI

The Flask web UI (`tools/federated/app_inter.py`) uses these core functions:

```python
from dsi.utils.federated import (
    discover_endpoints_async,
    download_csv_files_async,
    download_database_file_async,
)

# Step 1: Discovery
endpoints = asyncio.run(discover_endpoints_async(...))

# Step 2: Download CSV files
files = asyncio.run(download_csv_files_async(...))

# Step 3: Download databases  
db_file = asyncio.run(download_database_file_async(...))
```

The UI wraps these core functions with:
- Session management
- Credential storage
- Jump host mapping tracking
- HTTP request/response handling

## Dependencies

- `asyncssh` - Async SSH/SFTP operations
- `asyncio` - Async runtime

## Future Enhancements

Potential additions to this package:
- CLI tool using these functions
- Batch download utilities
- Progress reporting callbacks
- Connection pooling for multiple downloads
- Support for other authentication methods
