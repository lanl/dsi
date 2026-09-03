# DSI Data Federation Tool - Web UI

A user-friendly web interface for federating data endpoints from remote HPC systems.

## Features

- **🔍 Multi-Run Endpoint Discovery**: Discover endpoints from different HPCs with different configurations
- **📦 Endpoint Accumulation**: Run discovery multiple times, each run adds to your collection
- **🗑️ Flexible Management**: Add/remove individual endpoints or entire clusters before federating
- **📥 Batch Federation**: Federate all accumulated endpoints in one operation
- **📊 Progress Tracking**: Real-time feedback on discovery and federation operations
- **📝 Logging**: Comprehensive logging with web-based log viewer
- **🎨 Modern UI**: Clean, responsive interface with visual feedback

## Installation

1. Make sure you have the DSI package installed and accessible
2. Install the required dependencies:

```bash
cd tools/federated
pip install -r requirements.txt
```

## Usage

### Starting the Server

```bash
python app.py
```

The application will be available at: `http://localhost:5000`

### Step 1: Discover Endpoints (Repeatable)

**Important**: You can run this step **multiple times** with different configurations. Each run adds more endpoints to your collection.

#### First Discovery Run:
1. Fill in the HPC connection details:
   - **HPC Type**: Type of HPC system (hpc, kerberos, etc.)
   - **HPC Hostnames**: Enter one or more HPC hostnames, **one per line**. For example:
     ```
     cluster1.lanl.gov
     ```
   - **Username**: Your username on the HPC
   - **Script Path**: Path to the script that sets endpoint environment variables
   - **Endpoint Prefixes**: Comma-separated list of env variable prefixes (default: `DSI_ENDPOINT_,DIANA_ENDPOINT_`)

2. Click "🔍 Discover & Add to Collection"

3. Discovered endpoints are added to your collection and displayed in the **"Accumulated Endpoints"** section

#### Additional Discovery Runs:
4. **Change the form fields** for a different cluster (e.g., different hostname, different script path, different prefixes)

5. Click "🔍 Discover & Add to Collection" again

6. The new endpoints are **added** to your existing collection (not replacing them)

7. Repeat as many times as needed for different clusters or configurations

#### Managing Your Collection:
- View all accumulated endpoints grouped by cluster
- Remove individual endpoints with the "Remove" button
- Remove all endpoints from a cluster with "Remove All"
- Clear your entire collection with "🗑️ Clear All"
- Use "🔄 Reset Form" to clear the form fields

**Note**: The tool uses system SSH with Kerberos authentication. Ensure you have a valid Kerberos ticket before using (run `klist` to check, `kinit` or `reticket` to obtain).

### Step 2: Federate Data

1. Once you've accumulated all the endpoints you need, switch to the "2. Federate Data" tab
2. Review **all accumulated endpoints** from all discovery runs (organized by cluster)
3. Enter the local workspace folder where you want to federate the data
4. Click "📥 Federate Data"
5. The system will:
   - Download CSV files from each endpoint
   - Parse the CSV files to find database entries
   - **Prompt for credentials in the terminal** when accessing HPC databases
   - Federate all data to your workspace folder
6. Results will show:
   - Total number of clusters processed
   - Total databases successfully federated
   - Databases grouped by source cluster
   - Local paths for all federated data

**Important Notes**:
- The CSV files at each endpoint contain multiple database entries
- You'll be prompted for credentials in the **server terminal** (where you ran `python app.py`) when needed
- You can remove unwanted endpoints before federating by going back to the "Discover Endpoints" tab
- The UI shows results after federation completes

### Step 3: View Logs

1. Switch to the "3. View Logs" tab
2. Select a log session from the dropdown
3. View detailed logs of the discovery and federation process

## Example Workflow

Here's a typical workflow using multiple clusters with different configurations:

### Run 1: Discover from Cluster 1
```
HPC Type: hpc
HPC Hostname: cluster1.lanl.gov
Username: myusername
Script Path: /users/myusername/dsi_endpoints_prod.sh
Prefixes: DSI_ENDPOINT_,DIANA_ENDPOINT_

→ Click "Discover & Add to Collection"
→ Result: 5 endpoints added to collection
```

### Run 2: Discover from Cluster 2 (different script)
```
HPC Type: hpc
HPC Hostname: cluster2.lanl.gov
Username: myusername
Script Path: /projects/shared/dsi_endpoints_dev.sh
Prefixes: DSI_ENDPOINT_

→ Click "Discover & Add to Collection"
→ Result: 3 endpoints added (now 8 total)
```

### Run 3: Discover from Cluster 3 (different prefixes)
```
HPC Type: kerberos
HPC Hostname: cluster3.lanl.gov
Username: myusername
Script Path: /home/myusername/custom_endpoints.sh
Prefixes: CUSTOM_ENDPOINT_,TEST_ENDPOINT_

→ Click "Discover & Add to Collection"
→ Result: 2 endpoints added (now 10 total)
```

### Federation
```
→ Switch to "Federate Data" tab
→ Review all 10 accumulated endpoints
→ Enter workspace folder: ./my_federated_data
→ Click "Federate Data"
→ Terminal prompts for credentials when needed
→ All CSV files are downloaded and parsed
→ All databases from all endpoints are federated
```

## Configuration

### Endpoint Script Format

The script on the remote HPC should export environment variables with the configured prefixes:

```bash
# Example: load_dsi_endpoints.sh
export DSI_ENDPOINT_CHICOMA_1="/users/pascalgrosset/dsi_test/dsi_hpc_sources.csv"
export DSI_ENDPOINT_CHICOMA_2="/users/pascalgrosset/dsi_test/dsi_online_sources.csv"
export DIANA_ENDPOINT_DATA="/path/to/diana_data.csv"
```

### CSV File Format

The CSV files referenced by the endpoints should follow this format:

```csv
location_type,location,path,type,submitter_name,submitter_email,timestamp
HPC,ch-fe.lanl.gov,/lustre/scratch5/pascalgrosset/test_db/nif.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-3-10--16:38:00
url,url,https://oceans11.lanl.gov/dataCatalog/oceans11.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:30:00
```

## API Endpoints

### POST /api/discover-endpoints
Discover endpoints from one or more remote HPC systems

**Request Body:**
```json
{
  "hpc_type": "hpc",
  "hpc_names": ["cluster1.lanl.gov", "cluster2.lanl.gov", "cluster3.lanl.gov"],
  "username": "myusername",
  "script_path": "/path/to/script.sh",
  "prefixes": ["DSI_ENDPOINT_", "DIANA_ENDPOINT_"]
}
```

**Response:**
```json
{
  "success": true,
  "endpoints": {
    "cluster1.lanl.gov::ENDPOINT_1": "/path/to/file1.csv",
    "cluster2.lanl.gov::ENDPOINT_1": "/path/to/file2.csv"
  },
  "cluster_results": {
    "cluster1.lanl.gov": {"success": true, "count": 2, "endpoints": {...}},
    "cluster2.lanl.gov": {"success": true, "count": 1, "endpoints": {...}}
  },
  "total_clusters": 3,
  "successful_clusters": 2,
  "failed_clusters": ["cluster3.lanl.gov"],
  "session_id": "20260903_120000"
}
```

### POST /api/federate
Federate data from discovered endpoints (across multiple clusters)

**Request Body:**
```json
{
  "session_id": "20260903_120000",
  "endpoints_location": {
    "cluster1.lanl.gov::ENDPOINT_1": "/path/to/file1.csv",
    "cluster2.lanl.gov::ENDPOINT_1": "/path/to/file2.csv"
  },
  "workspace_folder": "./my_workspace"
}
```

**Note**: Credentials are prompted in the server terminal when needed for HPC database access.

**Response:**
```json
{
  "success": true,
  "success_count": 5,
  "clusters_processed": 2,
  "database_info": [
    {
      "name": "database.db",
      "source_cluster": "cluster1.lanl.gov",
      "local_path": "/path/to/workspace/hash",
      "original_path": "/remote/path/database.db"
    }
  ],
  "workspace_folder": "/absolute/path/to/workspace"
}
```

### GET /api/logs
List all available log files

### GET /api/logs/<session_id>
Get the content of a specific log file

## Logs

Logs are stored in the `tools/federated/logs/` directory. Each session generates a unique log file with the format:
- `federate_YYYYMMDD_HHMMSS.log`

## Security Notes

- The tool uses system SSH with Kerberos authentication
- Ensure you have a valid Kerberos ticket before running discovery operations
- The application runs on `0.0.0.0` by default. Restrict access in production environments
- For production use, configure HTTPS to encrypt web traffic

## Troubleshooting

### Connection Issues
- Verify you can SSH to the HPC manually
- Check that you have a valid Kerberos ticket (run `klist` to verify)
- If your ticket is expired, run `kinit` or `reticket` to obtain a new one
- Check that the script path is correct and accessible

### No Endpoints Found
- Verify the script exports environment variables with the correct prefixes
- Check the log files for detailed error messages
- Ensure the script is executable and runs without errors

### Federation Failures
- Check network connectivity to the HPC
- Verify file paths in the CSV files are correct
- Review logs for specific error messages

## Development

To run in development mode:

```bash
export FLASK_ENV=development
python app.py
```

The application will automatically reload when code changes are detected.
