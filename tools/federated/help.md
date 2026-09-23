# Get started

The role of this interface is to allow you to pull data from a number of different locations.

## Steps
1. **Workspace**: set a workspace folder in the left sidebar (step ①). This is where downloaded databases land, and it's also used to browse previously downloaded databases.
2. **Fetch Endpoint**: click "Fetch endpoints from a destination..." in the left sidebar, fill in the destination HPC and click "Load Endpoints".
3. **Endpoints**: click one of the discovered endpoints, in the left to see what data sources it exposes. Enter your credentials
4. **Choose a database**: pick a specific database from the endpoint and download it into your workspace.
5. **Explore**: select a downloaded database in the left sidebar to browse tables and run queries.


### Endpoints
For DSI, data can be accessed via endpoints. Endpoints mark the location of where data sources exist. Typically, endpoint are environment variablies prefixed with DSI_ENDPOINT_, DIANA_ENDPOINT_, or ... 


### Endpoint Script Path
Ideally, endpoints exist as environment variable that an HPC module loads. However, while in development, these are scripts. The 'endpoint script path' points to these files. e.g.

load_dsi_endpoints.sh:
```bash
export DSI_ENDPOINT_HPC="/users/pascalgrosset/dsi_test/dsi_hpc_sources.csv"
export DSI_ENDPOINT_ONLINE="/users/pascalgrosset/dsi_test/dsi_online_sources.csv"
```

Each of these endpoints points to a list of databases, e.g.
```csv
location_type,location,path,type,submitter_name,submitter_email,timestamp
url,url,https://www.timestored.com/data/sample/sakila.db,data,unknown,unknown,2026-3-10--16:38:00
url,url,https://oceans11.lanl.gov/dataCatalog/oceans11.db,data,pascal grosset,pascalgrosset@lanl.gov,2026-2-10--16:30:00
```


### Kerberos / jump host access

Some HPC systems require going through a jump host with Kerberos authentication:

- SSH to the jump host with your jump-host username/password.
- Run a ticket-init command on the jump host (defaults to `reticket`, but you can override it — e.g. to `kinit` — in the "Kerberos Init Command" field next to the jump host fields).
- SSH from the jump host to the target HPC using the resulting Kerberos ticket.

