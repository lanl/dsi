# DSI Data Federation Tool - Interactive Version

**UI-based credential collection - No terminal interaction required!**

This version provides a fully web-based workflow where credentials are collected through the UI instead of terminal prompts.

## Key Difference from Standard Version

| Feature | Standard (`app.py`) | Interactive (`app_inter.py`) |
|---------|---------------------|------------------------------|
| **Credential Input** | Terminal prompts | Web UI forms |
| **Port** | 5000 | 5001 |
| **Workflow** | 2-step | 3-step with analysis phase |
| **User Experience** | Must watch terminal | Everything in browser |
| **Federation** | Blocking | Non-blocking |

## How It Works

### 🔄 Three-Phase Workflow

#### Phase 1: Discovery (Same as standard)
- Discover endpoints from multiple HPC clusters
- Accumulate endpoints over multiple runs
- Manage your endpoint collection

#### Phase 2: Analysis (**NEW**)
1. Click "📊 Analyze Databases"
2. Backend downloads all CSV endpoint files
3. Parses CSVs to find all databases
4. Returns list of databases grouped by HPC location
5. UI dynamically generates credential form

#### Phase 3: Federation
1. UI shows all databases that need credentials
2. You provide credentials for each HPC location in the form
3. Click "📥 Start Federation"
4. Backend downloads all databases using provided credentials
5. Results shown in UI (no terminal watching needed!)

## Installation & Usage

### Starting the Server

```bash
cd tools/federated
python app_inter.py
```

The application will be available at: `http://localhost:5001`

## Example Workflow

### Step 1: Discover Endpoints
```
Cluster: ro-rfe.lanl.gov
Script: /path/to/load_dsi_endpoints.sh
→ Discovers 2 endpoints with CSV files
```

### Step 2: Analyze
```
→ Click "Analyze Databases"
→ Downloads CSV files
→ Finds:
  - darwin-fe.lanl.gov: 8 databases
  - ch-fe.lanl.gov: 5 databases
  - 2 URL databases (no credentials needed)
```

### Step 3: Provide Credentials in UI
```
UI shows:

┌─────────────────────────────────────────┐
│ HPC Credentials Required                │
│                                         │
│ 🖥️ darwin-fe.lanl.gov (8 databases)   │
│   Username: [pascalgrosset___]         │
│   Password: [**************]           │
│                                         │
│ 🖥️ ch-fe.lanl.gov (5 databases)       │
│   Username: [pascalgrosset___]         │
│   Password: [**************]           │
│                                         │
│ [📥 Start Federation]                  │
└─────────────────────────────────────────┘
```

### Step 4: Execute
```
→ Click "Start Federation"
→ All 13 databases download automatically
→ Results shown in browser
→ No terminal interaction!
```

## API Endpoints

### POST /api/analyze-endpoints (NEW)
Analyze endpoints and return database list

**Request:**
```json
{
  "session_id": "20260903_120000",
  "endpoints_location": {
    "ro-rfe.lanl.gov::DSI_ENDPOINT_1": "/path/to/file.csv"
  }
}
```

**Response:**
```json
{
  "success": true,
  "databases_by_cluster": {
    "ro-rfe.lanl.gov": {
      "hpc_by_location": {
        "darwin-fe.lanl.gov": [
          {
            "location": "darwin-fe.lanl.gov",
            "path": "/users/pulido/modelcard2.db",
            "type": "model"
          }
        ]
      },
      "total_hpc": 8,
      "total_url": 2,
      "total_s3": 0
    }
  },
  "totals": {
    "hpc": 8,
    "url": 2,
    "s3": 0,
    "total": 10
  }
}
```

### POST /api/federate (Modified)
Execute federation with UI-provided credentials

**Request:**
```json
{
  "session_id": "20260903_120000",
  "endpoints_location": {...},
  "workspace_folder": "./my_workspace",
  "credentials": {
    "darwin-fe.lanl.gov": {
      "username": "pascalgrosset",
      "password": "mypassword"
    },
    "ch-fe.lanl.gov": {
      "username": "pascalgrosset",
      "password": "mypassword"
    }
  }
}
```

## Benefits

### ✅ For Users
- **No terminal watching** - everything happens in the browser
- **See before you act** - know exactly what will be downloaded
- **Grouped credentials** - enter once per HPC location, not per database
- **Better visibility** - clear feedback on what's happening

### ✅ For Automation
- **Non-blocking** - can be called from scripts with credentials
- **Predictable** - no interactive prompts to handle
- **Complete API** - fully machine-readable responses

## Comparison Example

### Standard Version (`app.py` on port 5000)
```
1. Discover endpoints → UI
2. Click "Federate" → UI
3. Watch terminal for prompts → Terminal
4. Enter credentials 15 times → Terminal
5. Check browser for results → UI
```

### Interactive Version (`app_inter.py` on port 5001)
```
1. Discover endpoints → UI
2. Click "Analyze" → UI shows what's needed
3. Enter credentials in form → UI (grouped!)
4. Click "Federate" → UI
5. See results → UI
```

## Technical Details

### Analysis Phase
- Creates temporary directory: `.analyze_{session_id}`
- Downloads CSV files via SSH
- Parses to extract database entries
- Groups by `location` field (HPC hostname)
- Cleans up temporary files
- Returns structured database inventory

### Federation Phase
- Creates temporary directory: `.federate_{session_id}`
- Re-downloads CSV files
- Matches credentials to databases by location
- Downloads each database with provided credentials
- No prompts, no blocking
- Comprehensive error handling per database

### Security Notes
- Credentials passed via HTTPS (configure SSL for production)
- Credentials never logged
- Temporary directories cleaned up after each phase
- Each database download isolated with try/catch

## When to Use Which Version

**Use Standard (`app.py`)** when:
- You prefer terminal-based credential entry
- You want simpler architecture
- You're working in a secure local environment

**Use Interactive (`app_inter.py`)** when:
- You want everything in the browser
- You need to show someone else what will be downloaded
- You're building automation on top
- You want to avoid context switching between terminal and browser

Both versions support the same core features:
- Multi-cluster discovery
- Endpoint accumulation
- Comprehensive logging
- Same DSI backend integration

Choose based on your preferred credential entry method!
