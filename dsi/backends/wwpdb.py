"""
wwPDB Webserver Backend for DSI

Read-only metadata-first backend that retrieves wwPDB/RCSB metadata
and exposes it as in-memory DSI tables.

Access modes
------------
1. Identifier-driven mode
   - DOI input, e.g. "10.2210/pdb1cbs/pdb"
   - PDB ID input, e.g. "1CBS"

2. Query-driven mode, closer to NDP
   - params={"keywords": "hemoglobin", "limit": 20}
   - query_artifacts({"keywords": "hemoglobin", "limit": 20})
   - query_artifacts("hemoglobin")

# POST is used for RCSB Search API because the query body can contain
# nested search criteria such as keywords, authors, and filters.

# GET is used for RCSB Data API because each PDB ID maps to a fixed
# metadata resource URL.

REST flow
---------
RCSB Search API -> PDB IDs -> RCSB Data API -> normalized DSI tables

Tables
------
- datasets
- resources
- errors

self.tables["datasets"] = Tier 1 datasets
self.tables["resources"] = Tier 2 normalized resources
self.tables["errors"] = failed/skipped lookups

Current scope
-------------
- Metadata-first
- Read-only
- REST APIs only
- Exposes mmCIF download URLs
- Does not parse raw mmCIF content yet

"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dsi.backends.webserver import Webserver


DATA_CORE_URL = "https://data.rcsb.org/rest/v1/core" #https://data.rcsb.org/index.html#data-api
SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"  #https://search.rcsb.org/index.html#search-api

ENDPOINTS = {
    "search": SEARCH_URL,
    "data_core": DATA_CORE_URL,
    "entry": f"{DATA_CORE_URL}/entry/{{pdb_id}}",
    "entry_landing": "https://www.rcsb.org/structure/{pdb_id}",
    "mmcif_gz": (
        "https://files.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/"
        "{subdir}/{pdb_id}.cif.gz"
    ),
}

DOI_REGEX = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.I)
WWPDB_DOI_REGEX = re.compile(r"10\.2210/pdb([a-z0-9]{4})/pdb", re.I)
PDB_ID_REGEX = re.compile(r"^[A-Za-z0-9]{4}$")

# The 3 in-memory tables and it's defined columns

#Tier-1 schema
DATASET_SCHEMA = [
    "dataset_id",
    "source_repository",
    "doi",
    "title",
    "description",
    "landing_page",
    "metadata_url",
    "experimental_method",
    "release_date",
    "revision_date",
    "resource_count",
    "usability_label",
    "api_status",
    "query_source",
    "raw_metadata",
    "notes",
]

#Tier-2 schema
RESOURCE_SCHEMA = [
    "resource_id",
    "dataset_id",
    "source_repository",
    "name",
    "download_url",
    "format",
    "resource_type",
    "source",
    "raw_metadata",
]

#Error schema
ERROR_SCHEMA = [
    "identifier",
    "normalized_identifier",
    "repo",
    "status",
    "endpoint_used",
    "endpoint_variables",
    "query_source",
    "notes",
]


class ValueObject:
    """Container used by find/find_table/find_column/find_cell."""

    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "t_name": self.t_name,
            "c_name": self.c_name,
            "row_num": self.row_num,
            "value": self.value,
            "type": self.type,
        }


@dataclass
class FileResource:
    label: Optional[str]
    url: str
    extension: Optional[str]
    source: str
    format_hint: Optional[str] = None


@dataclass
class WWPDBResolution:  #Internal normalized result object for one lookup.
    original_identifier: str
    normalized_identifier: str
    repo: str
    endpoint_used: Optional[str]
    endpoint_variables: Dict[str, Any] = field(default_factory=dict)
    status: str = "no_match"
    title: Optional[str] = None
    record_id: Optional[str] = None
    doi: Optional[str] = None
    metadata_url: Optional[str] = None
    landing_page_url: Optional[str] = None
    query_source: Optional[str] = None
    files: List[FileResource] = field(default_factory=list)
    raw_metadata: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


def classify_usability(exts: Iterable[Optional[str]]) -> str:  #Classifies resource formats.
    ext_set = {e.lower() for e in exts if e}

    if not ext_set:
        return "lookup_failed"

    tabular = {"csv", "tsv", "xlsx", "xls", "json", "xml", "txt", "parquet"}
    scientific = {"cif", "cif.gz", "nc", "h5", "hdf5", "cdf"}
    archive_only = {"zip", "tar", "tar.gz", "gz"}

    if ext_set & tabular:
        return "tabular_or_easy_parse"
    if ext_set & scientific:
        return "scientific_structured"
    if ext_set <= archive_only:
        return "archive_only"
    return "other_format"


class WWPDB(Webserver): # This is the main DSI backend class, it implements the DSI webserver interface.
    """
    wwPDB/RCSB metadata backend for DSI.

    Implements the DSI Webserver interface and exposes RCSB/wwPDB
    metadata as in-memory DSI tables.
    """

    SUPPORTED_PARAMS = {
        "keywords",
        "authors",
        "experimental_method",
        "limit",
        "start",
        "return_type",
    }

    def __init__( #initializes the backend
        self,
        url: Optional[str] = None,
        identifiers: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        verify: bool | str = True,
        auto_load: bool = True,
        retries: int = 3,
        validate_on_init: bool = True,
        **kwargs,
    ) -> None:
        self.url = (url or DATA_CORE_URL).rstrip("/")
        self.identifiers = list(dict.fromkeys(identifiers or []))
        self.params = params or {}
        self.timeout = timeout
        self.verify = verify
        self.retries = retries
        self.validate_on_init = validate_on_init
        self.kwargs = kwargs

        self.session = self._create_session(retries=retries)

        self.tables: Dict[str, List[Dict[str, Any]]] = {} #In-memory dictionary created here for main storage
        self.schemas: Dict[str, List[str]] = {
            "datasets": DATASET_SCHEMA,
            "resources": RESOURCE_SCHEMA,
            "errors": ERROR_SCHEMA,
        }
        self.raw_results: List[WWPDBResolution] = []
        self.last_search_response: Optional[Dict[str, Any]] = None

        if auto_load:
            if self.validate_on_init:
                self.validate_connection()

            if self.identifiers:
                self._load_initial_data()
            elif self.params:
                self._load_from_params(self.params)
            else:
                self.process_artifacts()

    def _create_session(self, retries: int) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "dsi-wwpdb-backend/1.0",
                "Accept": "application/json",
            }
        )

        retry_strategy = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD", "POST"),
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def validate_connection(self) -> bool: # This is a checking function (a GET request) to see if the RCSB Data API is reachable
        test_url = ENDPOINTS["entry"].format(pdb_id="1CBS")
        response = self.session.get(test_url, timeout=self.timeout, verify=self.verify)
        response.raise_for_status()
        return True

    def _request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generic GET helper.

        Used for the RCSB Data API because each PDB ID maps to a fixed
        metadata resource URL.
        """
        response = self.session.get(
            endpoint,
            params=params,
            timeout=self.timeout,
            verify=self.verify,
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(f"RCSB response was not valid JSON: {endpoint}") from exc

    def _post_json(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST helper.

        Used for the RCSB Search API because the search request can contain
        nested keyword/filter logic in the JSON body.
        """
        response = self.session.post(
            endpoint,
            json=payload,
            timeout=self.timeout,
            verify=self.verify,
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(f"RCSB response was not valid JSON: {endpoint}") from exc

    @staticmethod
    def normalize_doi(value: Any) -> Optional[str]:  # Cleans DOI input
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            s = s.replace(prefix, "")

        match = DOI_REGEX.search(s)
        return match.group(1).lower().rstrip(" .;,)") if match else None

    @staticmethod
    def normalize_pdb_id(value: Any) -> Optional[str]:
        if value is None:
            return None

        s = str(value).strip()
        if PDB_ID_REGEX.match(s):
            return s.upper()

        return None

    @staticmethod
    def classify_identifier(identifier: Any) -> str:  #Decides on the input type
        doi = WWPDB.normalize_doi(identifier)
        if doi and WWPDB_DOI_REGEX.search(doi):
            return "wwpdb_doi"

        pdb_id = WWPDB.normalize_pdb_id(identifier)
        if pdb_id:
            return "pdb_id"

        return "other"

    @staticmethod
    def extract_pdb_id_from_doi(doi: str) -> Optional[str]:
        match = WWPDB_DOI_REGEX.search(doi)
        return match.group(1).upper() if match else None

    @staticmethod
    def get_file_ext(name_or_url: Optional[str]) -> Optional[str]:
        if not isinstance(name_or_url, str) or "." not in name_or_url:
            return None

        tail = name_or_url.split("?")[0].split("/")[-1].lower()
        parts = tail.split(".")

        if len(parts) >= 3 and parts[-1] == "gz":
            return ".".join(parts[-2:])

        return parts[-1]

    def _load_from_params(self, params: Dict[str, Any]) -> None:   # query driven loading and handling
        pdb_ids = self._search_rcsb(params)
        self.identifiers = pdb_ids
        self.raw_results = [
            self.lookup_identifier(pdb_id, query_source="params")
            for pdb_id in self.identifiers
        ]
        self.process_artifacts()

    def _search_rcsb(self, params: Dict[str, Any]) -> List[str]:
        """
        Search RCSB and return PDB IDs.

        Supported params:
        - keywords
        - authors
        - experimental_method
        - limit
        - start
        - return_type
        """
        self._validate_params(params)

        limit = int(params.get("limit", 100))
        start = int(params.get("start", 0))
        return_type = params.get("return_type", "entry")

        query_node = self._build_search_query(params)
        if query_node is None:
            return []

        payload = {
            "query": query_node,
            "return_type": return_type,
            "request_options": {
                "paginate": {
                    "start": start,
                    "rows": limit,
                }
            },
        }

        data = self._post_json(ENDPOINTS["search"], payload)
        self.last_search_response = data

        result_set = data.get("result_set", [])
        identifiers = []

        for item in result_set:
            identifier = item.get("identifier")
            if not identifier:
                continue
            identifiers.append(str(identifier).split("_")[0].split("-")[0].upper())

        return list(dict.fromkeys(identifiers))

    def _validate_params(self, params: Dict[str, Any]) -> None:
        unsupported = set(params.keys()) - self.SUPPORTED_PARAMS
        if unsupported:
            raise ValueError(
                f"Unsupported wwPDB search params: {sorted(unsupported)}. "
                f"Supported params: {sorted(self.SUPPORTED_PARAMS)}"
            )

    def _build_search_query(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        nodes: List[Dict[str, Any]] = []

        keywords = params.get("keywords")
        if keywords:
            nodes.append(
                {
                    "type": "terminal",
                    "service": "full_text",
                    "parameters": {"value": str(keywords)},
                }
            )

        authors = params.get("authors")
        if authors:
            nodes.append(
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_primary_citation.rcsb_authors",
                        "operator": "contains_phrase",
                        "value": str(authors),
                    },
                }
            )

        experimental_method = params.get("experimental_method")
        if experimental_method:
            nodes.append(
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "exptl.method",
                        "operator": "exact_match",
                        "value": str(experimental_method),
                    },
                }
            )

        if not nodes:
            return None

        if len(nodes) == 1:
            return nodes[0]

        return {
            "type": "group",
            "logical_operator": "and",
            "nodes": nodes,
        }

    def lookup_identifier( #Takes one input and decides how to process it 
        self,
        identifier: str,
        query_source: Optional[str] = None,
    ) -> WWPDBResolution:
        kind = self.classify_identifier(identifier)

        if kind == "wwpdb_doi":
            doi = self.normalize_doi(identifier)
            pdb_id = self.extract_pdb_id_from_doi(doi)
            return self.lookup_wwpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=doi,
                query_source=query_source or "identifier",
            )

        if kind == "pdb_id":
            pdb_id = self.normalize_pdb_id(identifier)
            return self.lookup_wwpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=None,
                query_source=query_source or "identifier",
            )

        return WWPDBResolution(
            original_identifier=str(identifier),
            normalized_identifier=str(identifier),
            repo="other",
            endpoint_used=None,
            status="skipped",
            query_source=query_source,
            notes=["Identifier did not match a wwPDB DOI or 4-character PDB ID."],
        )

    def lookup_wwpdb( # core metadata fetch function
        self,
        pdb_id: Optional[str],
        original_identifier: str,
        doi: Optional[str] = None,
        query_source: Optional[str] = None,
    ) -> WWPDBResolution:
        result = WWPDBResolution(
            original_identifier=original_identifier,
            normalized_identifier=doi or pdb_id or str(original_identifier),
            repo="wwpdb",
            endpoint_used=ENDPOINTS["entry"],
            endpoint_variables={"pdb_id": pdb_id},
            doi=doi,
            query_source=query_source,
        )

        if not pdb_id:
            result.notes.append("Could not resolve a 4-character PDB ID.")
            return result

        meta_url = ENDPOINTS["entry"].format(pdb_id=pdb_id)
        subdir = pdb_id.lower()[1:3]
        cif_url = ENDPOINTS["mmcif_gz"].format(subdir=subdir, pdb_id=pdb_id.lower())
        landing_url = ENDPOINTS["entry_landing"].format(pdb_id=pdb_id)

        result.metadata_url = meta_url
        result.landing_page_url = landing_url
        result.endpoint_variables["subdir"] = subdir

        try:
            meta = self._request(meta_url)

            result.status = "ok"
            result.title = meta.get("struct", {}).get("title")
            result.record_id = pdb_id

            result.raw_metadata = {
                "entry": pdb_id,
                "title": result.title,
                "experimental_methods": meta.get("exptl", []),
                "keywords": meta.get("struct_keywords", {}),
                "rcsb_accession_info": meta.get("rcsb_accession_info", {}),
                "citation": meta.get("citation", []),
            }

            result.files = [
                FileResource(
                    label=f"{pdb_id.lower()}.cif.gz",
                    url=cif_url,
                    extension="cif.gz",
                    source="wwpdb.archive_path",
                    format_hint="mmCIF",
                )
            ]

            return result

        except requests.HTTPError as exc:
            result.status = f"http_error_{getattr(exc.response, 'status_code', 'unknown')}"
            result.notes.append("RCSB entry metadata request failed.")
            return result

        except requests.RequestException as exc:
            result.status = "request_failed"
            result.notes.append(f"wwPDB request error: {str(exc)}")
            return result

        except Exception as exc:
            result.status = "parse_failed"
            result.notes.append(f"wwPDB parsing error: {str(exc)}")
            return result

    def _load_initial_data(self) -> None:
        self.raw_results = [
            self.lookup_identifier(identifier, query_source="identifier")
            for identifier in self.identifiers
        ]
        self.process_artifacts()

    def _extract_tables(self, results: List[WWPDBResolution]) -> Dict[str, List[Dict[str, Any]]]:   #Converts internal WWPDBResolution objects into DSI tables.
        # Rows are built here. Where each API lookup result is converted into row dictionaries
        datasets: List[Dict[str, Any]] = []
        resources: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for res in results:
            if res.status != "ok":
                errors.append(
                    {
                        "identifier": res.original_identifier,
                        "normalized_identifier": res.normalized_identifier,
                        "repo": res.repo,
                        "status": res.status,
                        "endpoint_used": res.endpoint_used,
                        "endpoint_variables": res.endpoint_variables,
                        "query_source": res.query_source,
                        "notes": " | ".join(res.notes) if res.notes else None,
                    }
                )
                continue

            file_exts = [f.extension for f in res.files]

            datasets.append(
                {
                    "dataset_id": res.record_id,
                    "source_repository": "wwPDB",
                    "doi": res.doi,
                    "title": res.title,
                    "description": res.title,
                    "landing_page": res.landing_page_url,
                    "metadata_url": res.metadata_url,
                    "experimental_method": self._extract_experimental_method(res.raw_metadata),
                    "release_date": self._extract_release_date(res.raw_metadata),
                    "revision_date": self._extract_revision_date(res.raw_metadata),
                    "resource_count": len(res.files),
                    "usability_label": classify_usability(file_exts),
                    "api_status": res.status,
                    "query_source": res.query_source,
                    "raw_metadata": res.raw_metadata,
                    "notes": " | ".join(res.notes) if res.notes else None,
                }
            )

            for idx, file_obj in enumerate(res.files, start=1):
                resources.append(
                    {
                        "resource_id": f"{res.record_id}:{idx}",
                        "dataset_id": res.record_id,
                        "source_repository": "wwPDB",
                        "name": file_obj.label,
                        "download_url": file_obj.url,
                        "format": file_obj.extension,
                        "resource_type": file_obj.format_hint,
                        "source": file_obj.source,
                        "raw_metadata": asdict(file_obj),
                    }
                )

        return { #And the final table dictionary is returned here.
            "datasets": self._apply_schema(datasets, DATASET_SCHEMA),
            "resources": self._apply_schema(resources, RESOURCE_SCHEMA),
            "errors": self._apply_schema(errors, ERROR_SCHEMA),
        }

    def _apply_schema(self, rows: List[Dict[str, Any]], schema: List[str]) -> List[Dict[str, Any]]:
        return [{column: row.get(column) for column in schema} for row in rows]

    def _rows_to_table(self, rows: List[Dict[str, Any]]):
        return rows

    def _resolve_table_name(self, table_name: Optional[str]) -> Optional[str]:
        if table_name is None:
            return None

        aliases = {
            "dataset": "datasets",
            "datasets": "datasets",
            "resource": "resources",
            "resources": "resources",
            "error": "errors",
            "errors": "errors",
        }
        return aliases.get(str(table_name).lower(), str(table_name))

    @staticmethod
    def _extract_experimental_method(raw_metadata: Dict[str, Any]) -> Optional[str]:
        methods = raw_metadata.get("experimental_methods", [])
        extracted = []

        for entry in methods:
            if isinstance(entry, dict) and entry.get("method"):
                extracted.append(entry["method"])

        return " | ".join(extracted) if extracted else None

    @staticmethod
    def _extract_release_date(raw_metadata: Dict[str, Any]) -> Optional[str]:
        accession_info = raw_metadata.get("rcsb_accession_info", {})
        return accession_info.get("initial_release_date") if isinstance(accession_info, dict) else None

    @staticmethod
    def _extract_revision_date(raw_metadata: Dict[str, Any]) -> Optional[str]:
        accession_info = raw_metadata.get("rcsb_accession_info", {})
        return accession_info.get("revision_date") if isinstance(accession_info, dict) else None

    def get_table(self, table_name: str):
        resolved = self._resolve_table_name(table_name)
        return self.tables.get(resolved, [])

    def get_tables(self):
        return self.tables

    def get_schema(self, table_name: Optional[str] = None):
        if table_name is None:
            return self.schemas
        resolved = self._resolve_table_name(table_name)
        return self.schemas.get(resolved, [])

    def get_table_names(self) -> List[str]:
        return list(self.tables.keys())

    def num_tables(self) -> int:
        return len(self.tables)

    def overwrite_table(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        resolved = self._resolve_table_name(table_name)
        schema = self.schemas.get(resolved, list(rows[0].keys()) if rows else [])
        self.tables[resolved] = self._rows_to_table(self._apply_schema(rows, schema))
        self.schemas[resolved] = schema

    def validate_urls(self, table_name: str = "resources", url_column: str = "download_url", **kwargs):
        rows = self.get_table(table_name)
        results = []

        for idx, row in enumerate(rows):
            url = row.get(url_column)
            is_valid = False
            status_code = None
            error = None
            method_used = None

            try:
                if not isinstance(url, str) or not url.strip():
                    error = "missing_url"
                else:
                    response = self.session.head(
                        url,
                        allow_redirects=True,
                        timeout=self.timeout,
                        verify=self.verify,
                    )
                    status_code = response.status_code
                    method_used = "HEAD"

                    if not (200 <= status_code < 400):
                        response = self.session.get(
                            url,
                            stream=True,
                            allow_redirects=True,
                            timeout=self.timeout,
                            verify=self.verify,
                        )
                        status_code = response.status_code
                        method_used = "GET_STREAM"

                    is_valid = 200 <= status_code < 400
                    response.close()

            except Exception as exc:
                error = str(exc)

            results.append(
                {
                    "row_num": idx,
                    "table_name": table_name,
                    "url": url,
                    "is_valid": is_valid,
                    "status_code": status_code,
                    "method_used": method_used,
                    "error": error,
                }
            )

        return results

    # ------------------------------------------------------------------
    # Webserver abstract interface methods
    # ------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        raise NotImplementedError("WWPDB backend is read-only.")

    def query_artifacts(self, query, **kwargs):
        if query is None:
            return self.tables

        if isinstance(query, dict):
            self.params = query
            self._load_from_params(query)
            return self.tables

        if isinstance(query, str):
            kind = self.classify_identifier(query)

            if kind in {"wwpdb_doi", "pdb_id"}:
                self.identifiers = [query]
                self._load_initial_data()
            else:
                self.params = {"keywords": query, **kwargs}
                self._load_from_params(self.params)

            return self.tables

        if isinstance(query, list):
            self.identifiers = query
            self._load_initial_data()
            return self.tables

        raise TypeError("query_artifacts expects None, str, list, or dict.")

    def notebook(self, **kwargs):
        table_name = kwargs.pop("table_name", None)
        return self.display(table_name, **kwargs)

    def process_artifacts(self, **kwargs):
        extracted = self._extract_tables(self.raw_results)
        self.tables = {}

        for table_name, rows in extracted.items():
            self.tables[table_name] = self._rows_to_table(rows)
            self.schemas[table_name] = list(rows[0].keys()) if rows else self.schemas.get(table_name, [])

        return self.tables

    def find(self, query_object, **kwargs):
        needle = str(query_object).lower()
        matches: List[ValueObject] = []

        for table_name, rows in self.tables.items():
            for row_num, row in enumerate(rows):
                for col_name, value in row.items():
                    if needle in str(value).lower():
                        vo = ValueObject()
                        vo.t_name = table_name
                        vo.c_name = [col_name]
                        vo.row_num = row_num
                        vo.value = value
                        vo.type = "cell"
                        matches.append(vo)

        return matches

    def find_table(self, query_object, **kwargs):
        needle = str(query_object).lower()
        matches: List[ValueObject] = []

        for table_name in self.tables:
            if needle in table_name.lower():
                vo = ValueObject()
                vo.t_name = table_name
                vo.value = table_name
                vo.type = "table"
                matches.append(vo)

        return matches

    def find_column(self, query_object, **kwargs):
        needle = str(query_object).lower()
        matches: List[ValueObject] = []

        for table_name, columns in self.schemas.items():
            for col_name in columns:
                if needle in col_name.lower():
                    vo = ValueObject()
                    vo.t_name = table_name
                    vo.c_name = [col_name]
                    vo.value = col_name
                    vo.type = "column"
                    matches.append(vo)

        return matches

    def find_cell(self, query_object, **kwargs):
        return self.find(query_object, **kwargs)

    def find_relation(self, column_name, relation, **kwargs):
        if column_name != "dataset_id":
            return []

        datasets = self.get_table("datasets")
        resources = self.get_table("resources")

        out = []
        for ds in datasets:
            dsid = ds.get("dataset_id")
            out.append(
                {
                    "dataset_id": dsid,
                    "relation": relation,
                    "related_resource_count": len(
                        [r for r in resources if r.get("dataset_id") == dsid]
                    ),
                }
            )
        return out

    def list(self, **kwargs):
        return self.get_table_names()

    def display(self, table_name=None, **kwargs):
        if table_name is None:
            return self.tables
        return self.get_table(table_name)

    def summary(self, table_name=None, **kwargs):
        if table_name is not None:
            rows = self.get_table(table_name)
            return {
                "backend": "WWPDB",
                "table_name": self._resolve_table_name(table_name),
                "row_count": len(rows),
                "columns": self.get_schema(table_name),
            }

        return {
            "backend": "WWPDB",
            "num_tables": self.num_tables(),
            "tables": {name: len(rows) for name, rows in self.tables.items()},
            "params": self.params,
            "identifier_count": len(self.identifiers),
        }

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()