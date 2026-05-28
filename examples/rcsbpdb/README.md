RCSBPDB Backend for DSI

A read-only metadata backend for accessing RCSB Protein Data Bank (RCSB PDB) metadata through REST APIs and exposing metadata as DSI-compatible tables: datasets, resources, and errors.

Note: This backend is read-only — it queries remote RCSB APIs and stores metadata in-memory as DSI tables.

Quick Start
from dsi.dsi import DSI

# Keyword search
db = DSI(
    backend_name="RCSBPDB",
    params={"keywords": "hemoglobin", "limit": 5},
)

# Access curated tables
datasets = db.get_table("datasets", collection=True)
resources = db.get_table("resources", collection=True)

print(datasets.columns)

db.close()
Features
Metadata Retrieval

Retrieve structural biology metadata directly from RCSB PDB APIs.

Supported access patterns:

Keyword / Search-driven retrieval
db = DSI(
    backend_name="RCSBPDB",
    params={"keywords": "kinase"}
)

Supported search parameters:

keywords
authors
experimental_method
limit
start
Identifier-driven retrieval
PDB ID lookup
db = DSI(
    backend_name="RCSBPDB",
    params={"pdbID": "1CBS"}
)
DOI lookup
db = DSI(
    backend_name="RCSBPDB",
    params={"DOI": "10.2210/pdb1cbs/pdb"}
)

Supported identifier aliases:

pdb_id
pdbID
pdbId
PDB_ID
pdbid
PDBID
doi
DOI
identifiers
API Flow
Search-driven flow
User params
    ↓
RCSB Search API
    ↓
PDB IDs
    ↓
RCSB Data API
    ↓
Normalized DSI tables
Identifier-driven flow
PDB ID / DOI
    ↓
Identifier normalization
    ↓
RCSB Data API
    ↓
Normalized DSI tables
Tables

The backend creates three curated DSI tables:

datasets

Primary metadata table containing curated dataset-level fields.

Example columns:

dataset_id
doi
title
experimental_method
release_date
revision_date
landing_page
metadata_url
resource_count
raw_metadata
resources

Normalized downloadable resource table.

Example columns:

resource_id
dataset_id
download_url
format
resource_type
errors

Failed lookup or parsing records.

Example columns:

identifier
status
endpoint_used
notes
Curated Metadata + Full Metadata

The backend stores metadata in two layers:

1. Curated relational schema

Important metadata fields are extracted into searchable DSI columns inside:

datasets
resources
errors

These fields are optimized for:

querying
filtering
searching
summarization
provenance tracking
2. Full raw metadata JSON

The complete RCSB API response is preserved inside:

raw_metadata["full_metadata"]

This ensures:

no metadata loss
future compatibility
access to all original API fields
support for advanced domain-specific workflows
Example Access
datasets = db.get_table("datasets", collection=True)

# Access curated field
print(datasets["title"])

# Access full raw metadata
print(datasets["raw_metadata"][0]["full_metadata"])
Resource Downloads

The backend automatically exposes downloadable mmCIF structure files.

Example resource URL:

https://files.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/
In-Memory Search Utilities

Supported utilities:

search()
find()
find_table()
find_column()
find_cell()

Example:

db.search("X-RAY")
Inspection Utilities
List tables
db.list()
View schema
db.schema()
Summarize backend
db.summary()
Examples

Run the RCSBPDB examples:

examples/rcsbpdb/1.load_basic.py
examples/rcsbpdb/2.list_tables.py
examples/rcsbpdb/3.get_tables.py
examples/rcsbpdb/4.search.py
examples/rcsbpdb/5.filter_data.py
examples/rcsbpdb/6.get_columns.py
Notes
The backend is metadata-first and read-only.
query_artifacts() replaces previously loaded in-memory data.
General publication DOI search is not currently supported.
Only RCSB-style DOIs of the form:
10.2210/pdbXXXX/pdb

are directly converted into PDB IDs.

Full metadata is preserved under:
raw_metadata["full_metadata"]
Curated tables provide simplified relational access for common workflows while preserving the original API JSON.