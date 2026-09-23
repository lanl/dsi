# DSI Federation: Old vs New

## Quick Comparison

| Feature | Old (`subprocess SSH`) | New (`dsi.utils.federated`) |
|---------|------------------------|----------------------------|
| **Method** | System `ssh` command | asyncssh library |
| **Password auth** | ❌ No | ✅ Yes |
| **Jump host** | ❌ No | ✅ Yes |
| **Auto reticket** | ❌ Manual | ✅ Automatic |
| **Location** | `data_acquisition.py` | `federated/` module |
| **Reusable** | ⚠️ Function only | ✅ Full module |
| **Async** | ❌ No | ✅ Yes |
| **Fallback** | ❌ No | ✅ Two methods |
| **Logging** | ⚠️ Basic | ✅ Detailed |

## Code Comparison

### Discovery

#### Old Way (Still Works!)
```python
from dsi.utils.data_acquisition import get_remote_endpoints_ssh

# Requires manual reticket before running
# Uses system SSH with Kerberos
endpoints = get_remote_endpoints_ssh(
    hostname="darwin-fe.lanl.gov",
    username="pascalgrosset",
    script_path="/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh",
    prefixes=['DSI_ENDPOINT_'],
    verbose=True
)
```

#### New Way (Enhanced!)
```python
from dsi.utils.data_acquisition import get_remote_endpoints_ssh

# Option 1: Direct SSH with password
endpoints = get_remote_endpoints_ssh(
    hostname="darwin-fe.lanl.gov",
    username="pascalgrosset",
    password="mypassword",  # NEW!
    script_path="/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh",
    prefixes=['DSI_ENDPOINT_'],
    verbose=True
)

# Option 2: Via jump host with automatic reticket
endpoints = get_remote_endpoints_ssh(
    hostname="tuolumne.llnl.gov",
    username="grosset2",
    hpc_type='kerberos',
    script_path="/g/g92/grosset2/dsi_test/load_dsi_endpoints.sh",
    prefixes=['DSI_ENDPOINT_'],
    jump_host="ro-rfe.lanl.gov",  # NEW!
    jump_username="pascalgrosset",  # NEW!
    jump_password="jumphost_pass",  # NEW!
    verbose=True
)
```

#### Direct Core Function (Maximum Control!)
```python
from dsi.utils.federated import discover_endpoints_async
import asyncio

endpoints = asyncio.run(discover_endpoints_async(
    hostname="tuolumne.llnl.gov",
    username="grosset2",
    script_path="/g/g92/grosset2/dsi_test/load_dsi_endpoints.sh",
    prefixes=['DSI_ENDPOINT_'],
    hpc_type='kerberos',
    jump_host="ro-rfe.lanl.gov",
    jump_username="pascalgrosset",
    jump_password="jumphost_pass",
    logger=my_logger  # Optional: pass your own logger
))
```

### File Downloads

#### Old Way
```python
# No jump host support!
# Had to manually handle Kerberos tickets
# Used DSI's pull_data() which doesn't support jump hosts

from dsi.utils.data_acquisition import pull_data

file_path = pull_data(
    location_type='hpc',
    remote_location='tuolumne.llnl.gov',
    remote_path='/g/g92/grosset2/dsi_test/simplefolks.sqlite',
    download_location='/tmp/databases',
    username='grosset2',
    password=''  # Relied on existing Kerberos ticket
)
# ❌ Would fail for jump host systems!
```

#### New Way
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
    jump_password="jumphost_pass",
    jump_host_required=True
))
# ✅ Handles jump host automatically!
```

## Authentication Flow Comparison

### Old: Manual Kerberos
```bash
# User had to do this manually BEFORE running script:
$ ssh pascalgrosset@ro-rfe.lanl.gov
$ reticket
$ # Keep this session open
$ # Run Python script in another terminal
$ python federate_enpoint.py
```

### New: Automatic Kerberos
```bash
# Everything automated:
$ python federate_enpoint.py
# Script automatically:
# 1. Connects to jump host
# 2. Runs reticket
# 3. Connects to final HPC
# 4. Downloads files
```

## Jump Host Pattern

### What the Code Does Automatically

```
You (Local) → Jump Host (ro-rfe.lanl.gov) → Final HPC (tuolumne.llnl.gov)
            password auth                  Kerberos (via reticket)
```

**Step-by-step:**
1. SSH to `ro-rfe.lanl.gov` with `pascalgrosset` + password
2. Run `reticket` on jump host to get Kerberos tickets
3. Verify tickets with `klist`
4. SSH from jump host to `tuolumne.llnl.gov` as `grosset2` using GSSAPI
5. Execute commands or download files

**Two-Method Fallback:**
- **Method 1 (Preferred):** asyncssh with `gss_auth=True` (GSSAPI)
- **Method 2 (Fallback):** Run `ssh` or `scp` command directly on jump host

## Migration Path

### Phase 1: No Changes Required ✅
Your existing scripts work as-is:
```python
# This exact code still works
endpoints = get_remote_endpoints_ssh(
    hostname="darwin-fe.lanl.gov",
    username="pascalgrosset",
    script_path="/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh"
)
```

### Phase 2: Add Jump Host Support (Optional)
When you need jump host access:
```python
endpoints = get_remote_endpoints_ssh(
    hostname="tuolumne.llnl.gov",
    username="grosset2",
    script_path="/g/g92/grosset2/dsi_test/load_dsi_endpoints.sh",
    hpc_type='kerberos',  # Add this
    jump_host="ro-rfe.lanl.gov",  # Add this
    jump_username="pascalgrosset",  # Add this
    jump_password="..."  # Add this
)
```

### Phase 3: Use Core Modules (Advanced)
For new tools/notebooks with full control:
```python
from dsi.utils.federated import discover_endpoints_async
import asyncio

endpoints = asyncio.run(discover_endpoints_async(...))
```

## Benefits Summary

### For CLI Users
- ✅ **Backward compatible** - old code still works
- ✅ **New features** - jump host support available when needed
- ✅ **No manual reticket** - automated Kerberos handling
- ✅ **Password auth** - works without Kerberos setup

### For Developers
- ✅ **Reusable modules** - core functions in `dsi.utils.federated`
- ✅ **Testable** - business logic separate from UI
- ✅ **Documented** - examples and docstrings
- ✅ **Async support** - for concurrent operations

### For System Administrators
- ✅ **More secure** - doesn't rely on long-lived Kerberos tickets
- ✅ **More flexible** - supports multiple authentication methods
- ✅ **Better logging** - detailed audit trail
- ✅ **Consistent** - same logic across CLI, UI, notebooks

## Troubleshooting

### Old System Issues
```
Problem: "Permission denied"
Reason: Kerberos ticket expired
Solution: Manually run reticket in jump host session
```

### New System Benefits
```
Same Problem: "Permission denied"
Reason: Automatically detected and logged
Solution: System auto-runs reticket and retries
```

## Performance Comparison

| Operation | Old (subprocess) | New (asyncssh) | Winner |
|-----------|------------------|----------------|--------|
| Single discovery | ~5s | ~5s | Tie |
| Single download | ~10s | ~10s | Tie |
| Multiple concurrent | N/A (sequential) | Async (parallel) | New ✅ |
| Error handling | Basic | Detailed + fallback | New ✅ |
| Logging | Minimal | Comprehensive | New ✅ |

## Future Enhancements Enabled

With the new modular design, we can easily add:

- ✅ Parallel discovery from multiple clusters
- ✅ Progress bars for long downloads
- ✅ Connection pooling
- ✅ Retry logic with exponential backoff
- ✅ Configuration file support (YAML)
- ✅ Dedicated CLI tool with argparse
- ✅ Integration testing framework
- ✅ Performance profiling

All without touching the UI code! 🎉
