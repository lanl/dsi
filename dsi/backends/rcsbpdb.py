"""
RCSBPDB Webserver Backend for DSI

Read-only metadata-first backend that retrieves rcsb pdb metadata
and exposes it as in-memory DSI tables.

Access modes
------------
1. Identifier-driven mode
   - DOI input, e.g. "10.2210/pdb1cbs/pdb"
   - PDB ID input, e.g. "1CBS"

2. Query-driven mode, closer to NDP
   - params={"keywords": "hemoglobin", "limit": 20}
   - find_relation({"keywords": "hemoglobin", "limit": 20})
   - find_relation("hemoglobin")

DOI behavior
------------
RCSB-style DOI input is supported through identifiers or params.
Only DOIs of the form 10.2210/pdbXXXX/pdb are converted directly into PDB IDs.
General publication DOI search is not currently supported.

REST flow
---------
RCSB Search API -> PDB IDs -> RCSB Data API -> normalized DSI tables

Tables
------
- datasets
- resources
- errors

Tier mapping
------------
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

identifiers
→ __init__()
→ _load_initial_data()
→ lookup_identifier()
→ lookup_rcsbpdb()
→ _request()
→ GET https://data.rcsb.org/rest/v1/core/entry/{pdb_id}

params
→ __init__()
→ _load_from_params()
→ _search_rcsb()
→ _build_search_query()
→ _post_json()
→ POST https://search.rcsb.org/rcsbsearch/v2/query
"""


from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional
from collections import OrderedDict

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dsi.backends.webserver import Webserver


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
    """Normalized downloadable file/resource representation."""

    label: Optional[str]
    url: str
    extension: Optional[str]
    source: str
    format_hint: Optional[str] = None
    exists: Optional[bool] = None
    status_code: Optional[int] = None
    content_type: Optional[str] = None


@dataclass
class RCSBPDBResolution:
    """Internal normalized result object for one rcsbpdb/RCSB lookup."""

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


class RCSBPDB(Webserver):
    """
    rcsbpdb/RCSB metadata backend for DSI.

    Implements the DSI Webserver interface and exposes RCSB/rcsbpdb
    metadata as in-memory DSI tables.
    """
    read_only = True
    DATA_CORE_URL = "https://data.rcsb.org/rest/v1/core"
    SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"

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
    RCSBPDB_DOI_REGEX = re.compile(r"10\.2210/pdb([a-z0-9]{4})/pdb", re.I)
    PDB_ID_REGEX = re.compile(r"^[A-Za-z0-9]{4}$")

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

    SUPPORTED_PARAMS = {
        "keywords",
        "authors",
        "experimental_method",
        "pdb_id",
        "pdbID",
        "pdbId",
        "PDB_ID",
        "pdbid",
        "PDBID",
        "doi",
        "DOI",
        "identifiers",
        "limit",
        "start",
        "return_type",
    }

    def __init__(
        self,
        url: Optional[str] = None,
        identifiers: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """
        Initialize rcsbpdb backend and optionally load data from RCSB APIs.
        """
        self.url = (url or self.DATA_CORE_URL).rstrip("/")
        self.identifiers = list(dict.fromkeys(identifiers or []))
        self.params = params or {}

        self.timeout = kwargs.get("timeout", 60)
        self.verify = kwargs.get("verify_ssl", kwargs.get("verify", True))
        self.validate_resource_urls = kwargs.get("validate_resource_urls", True)
        self.retries = kwargs.get("retries", 3)
        self.validate_on_init = kwargs.get("validate_on_init", True)
        self.auto_load = kwargs.get("auto_load", True)
        self.kwargs = kwargs

        self.session = self._create_session(retries=self.retries)

        self.tables: Dict[str, List[Dict[str, Any]]] = {}
        self.schemas: Dict[str, List[str]] = {
            "datasets": self.DATASET_SCHEMA,
            "resources": self.RESOURCE_SCHEMA,
            "errors": self.ERROR_SCHEMA,
        }

        self.raw_results: List[RCSBPDBResolution] = []
        self.last_search_response: Optional[Dict[str, Any]] = None
        self._loaded = False

        if self.validate_on_init:
            try:
                self.validate_connection()
            except Exception as exc:
                self._loaded = False
                raise RuntimeError(f"rcsbpdb connection validation failed: {exc}") from exc

        if self.auto_load:
            try:
                if self.identifiers:
                    self._load_initial_data()
                elif self.params:
                    self._load_from_params(self.params)
                else:
                    self.process_artifacts()

                self._loaded = True

            except Exception as exc:
                self._loaded = False
                raise RuntimeError(f"Failed to load initial rcsbpdb data: {exc}") from exc
        else:
            self._loaded = True

    # ------------------------------------------------------------------
    # HTTP/session helpers
    # ------------------------------------------------------------------
    def _create_session(self, retries: int) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "dsi-rcsbpdb-backend/1.0",
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

    def validate_connection(self) -> bool:
        test_url = self.ENDPOINTS["entry"].format(pdb_id="1CBS")
        response = self.session.get(test_url, timeout=self.timeout, verify=self.verify)
        response.raise_for_status()
        return True

    def _request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generic GET helper.
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
        Generic POST helper.
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

    # ------------------------------------------------------------------
    # Identifier helpers
    # ------------------------------------------------------------------
    @classmethod
    def normalize_doi(cls, value: Any) -> Optional[str]:
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            s = s.replace(prefix, "")

        match = cls.DOI_REGEX.search(s)
        return match.group(1).lower().rstrip(" .;,)") if match else None

    @classmethod
    def normalize_pdb_id(cls, value: Any) -> Optional[str]:
        if value is None:
            return None

        s = str(value).strip()
        if cls.PDB_ID_REGEX.match(s):
            return s.upper()

        return None

    @classmethod
    def classify_identifier(cls, identifier: Any) -> str:
        doi = cls.normalize_doi(identifier)
        if doi and cls.RCSBPDB_DOI_REGEX.search(doi):
            return "rcsbpdb_doi"

        pdb_id = cls.normalize_pdb_id(identifier)
        if pdb_id:
            return "pdb_id"

        return "other"

    @classmethod
    def extract_pdb_id_from_doi(cls, doi: str) -> Optional[str]:
        match = cls.RCSBPDB_DOI_REGEX.search(doi)
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

    @staticmethod
    def classify_usability(exts: Iterable[Optional[str]]) -> str:
        """
        Classify resource usability based on file extensions.
        """
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

    # ------------------------------------------------------------------
    # Query-driven Search API support
    # ------------------------------------------------------------------
    def _extract_identifiers_from_params(self, params: Dict[str, Any]) -> List[str]:
        """
        Extract DOI/PDB identifiers from params and normalize aliases.

        Supported identifier-style params:
        - identifiers
        - pdb_id, pdbID, pdbId, PDB_ID, pdbid, PDBID
        - doi, DOI
        """
        identifiers: List[str] = []

        for key in ("identifiers",):
            value = params.get(key)
            if value:
                if isinstance(value, list):
                    identifiers.extend(str(v) for v in value)
                else:
                    identifiers.append(str(value))

        for key in ("pdb_id", "pdbID", "pdbId", "PDB_ID", "pdbid", "PDBID"):
            value = params.get(key)
            if value:
                if isinstance(value, list):
                    identifiers.extend(str(v) for v in value)
                else:
                    identifiers.append(str(value))

        for key in ("doi", "DOI"):
            value = params.get(key)
            if value:
                if isinstance(value, list):
                    identifiers.extend(str(v) for v in value)
                else:
                    identifiers.append(str(value))

        return list(dict.fromkeys(identifiers))
    
    def _load_from_params(self, params: Dict[str, Any]) -> None:
        self._validate_params(params)

        identifiers = self._extract_identifiers_from_params(params)

        if identifiers:
            self.identifiers = identifiers
            self.raw_results = [
                self.lookup_identifier(identifier, query_source="params")
                for identifier in self.identifiers
            ]
            self.process_artifacts()
            return

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

        data = self._post_json(self.ENDPOINTS["search"], payload)
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
                f"Unsupported rcsbpdb search params: {sorted(unsupported)}. "
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

    # ------------------------------------------------------------------
    # Repository lookup and metadata retrieval
    # ------------------------------------------------------------------
    def lookup_identifier(
        self,
        identifier: str,
        query_source: Optional[str] = None,
    ) -> RCSBPDBResolution:
        kind = self.classify_identifier(identifier)

        if kind == "rcsbpdb_doi":
            doi = self.normalize_doi(identifier)
            pdb_id = self.extract_pdb_id_from_doi(doi)
            return self.lookup_rcsbpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=doi,
                query_source=query_source or "identifier",
            )

        if kind == "pdb_id":
            pdb_id = self.normalize_pdb_id(identifier)
            return self.lookup_rcsbpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=None,
                query_source=query_source or "identifier",
            )

        return RCSBPDBResolution(
            original_identifier=str(identifier),
            normalized_identifier=str(identifier),
            repo="other",
            endpoint_used=None,
            status="skipped",
            query_source=query_source,
            notes=["Identifier did not match a rcsbpdb DOI or 4-character PDB ID."],
        )

    def _url_exists(self, url: str):
        try:
            response = self.session.head(
                url,
                allow_redirects=True,
                timeout=self.timeout,
                verify=self.verify,
            )

            if response.status_code == 405:
                response = self.session.get(
                    url,
                    stream=True,
                    allow_redirects=True,
                    timeout=self.timeout,
                    verify=self.verify,
                )

            exists = 200 <= response.status_code < 400
            status_code = response.status_code
            content_type = response.headers.get("content-type")
            response.close()

            return exists, status_code, content_type

        except Exception:
            return None, None, None

    def _make_resource(
        self,
        pdb_id: str,
        label: str,
        url: str,
        extension: str,
        source: str,
        format_hint: str,
    ) -> Optional[FileResource]:
        exists = None
        status_code = None
        content_type = None

        if self.validate_resource_urls:
            exists, status_code, content_type = self._url_exists(url)

            if exists is False:
                return None

        return FileResource(
            label=label,
            url=url,
            extension=extension,
            source=source,
            format_hint=format_hint,
            exists=exists,
            status_code=status_code,
            content_type=content_type,
        )

    def _build_file_resources(self, pdb_id: str, meta: Dict[str, Any]) -> List[FileResource]:
        pdb_id_upper = pdb_id.upper()
        pdb_id_lower = pdb_id.lower()

        candidates = [
            (
                f"{pdb_id_lower}.cif",
                f"https://files.rcsb.org/download/{pdb_id_upper}.cif",
                "cif",
                "rcsb.download",
                "PDBx/mmCIF Format",
            ),
            (
                f"{pdb_id_lower}.cif.gz",
                f"https://files.rcsb.org/download/{pdb_id_upper}.cif.gz",
                "cif.gz",
                "rcsb.download",
                "PDBx/mmCIF Format (gz)",
            ),
            (
                f"{pdb_id_lower}.bcif.gz",
                f"https://files.rcsb.org/download/{pdb_id_upper}.bcif.gz",
                "bcif.gz",
                "rcsb.download",
                "BinaryCIF Format (gz)",
            ),
            (
                f"{pdb_id_lower}.pdb",
                f"https://files.rcsb.org/download/{pdb_id_upper}.pdb",
                "pdb",
                "rcsb.download",
                "Legacy PDB Format",
            ),
            (
                f"{pdb_id_lower}.pdb.gz",
                f"https://files.rcsb.org/download/{pdb_id_upper}.pdb.gz",
                "pdb.gz",
                "rcsb.download",
                "Legacy PDB Format (gz)",
            ),
            (
                f"{pdb_id_lower}.xml.gz",
                f"https://files.rcsb.org/download/{pdb_id_upper}.xml.gz",
                "xml.gz",
                "rcsb.download",
                "PDBML/XML Format (gz)",
            ),
            (
                f"{pdb_id_lower}-sf.cif",
                f"https://files.rcsb.org/download/{pdb_id_upper}-sf.cif",
                "cif",
                "rcsb.download",
                "Structure Factors (CIF)",
            ),
            (
                f"{pdb_id_lower}-sf.cif.gz",
                f"https://files.rcsb.org/download/{pdb_id_upper}-sf.cif.gz",
                "cif.gz",
                "rcsb.download",
                "Structure Factors (CIF - gz)",
            ),
        ]

        subdir = pdb_id_lower[1:3]

        validation_base = (
            f"https://files.rcsb.org/pub/pdb/validation_reports/"
            f"{subdir}/{pdb_id_lower}/{pdb_id_lower}"
        )

        candidates.extend(
            [
                (
                    f"{pdb_id_lower}_validation.pdf.gz",
                    f"{validation_base}_validation.pdf.gz",
                    "pdf.gz",
                    "rcsb.validation",
                    "Validation Full (PDF - gz)",
                ),
                (
                    f"{pdb_id_lower}_validation.xml.gz",
                    f"{validation_base}_validation.xml.gz",
                    "xml.gz",
                    "rcsb.validation",
                    "Validation (XML - gz)",
                ),
                (
                    f"{pdb_id_lower}_validation.cif.gz",
                    f"{validation_base}_validation.cif.gz",
                    "cif.gz",
                    "rcsb.validation",
                    "Validation (CIF - gz)",
                ),
            ]
        )

        assemblies = meta.get("pdbx_struct_assembly", [])
        assembly_ids = []

        if isinstance(assemblies, list):
            for assembly in assemblies:
                if isinstance(assembly, dict) and assembly.get("id"):
                    assembly_ids.append(str(assembly["id"]))

        for assembly_id in assembly_ids:
            candidates.extend(
                [
                    (
                        f"{pdb_id_lower}-assembly{assembly_id}.cif.gz",
                        f"https://files.rcsb.org/download/{pdb_id_upper}-assembly{assembly_id}.cif.gz",
                        "cif.gz",
                        "rcsb.assembly",
                        f"Biological Assembly {assembly_id} (CIF - gz)",
                    ),
                    (
                        f"{pdb_id_lower}-assembly{assembly_id}.pdb.gz",
                        f"https://files.rcsb.org/download/{pdb_id_upper}-assembly{assembly_id}.pdb.gz",
                        "pdb.gz",
                        "rcsb.assembly",
                        f"Biological Assembly {assembly_id} (PDB - gz)",
                    ),
                ]
            )

        resources = []

        for label, url, extension, source, format_hint in candidates:
            resource = self._make_resource(
                pdb_id=pdb_id_upper,
                label=label,
                url=url,
                extension=extension,
                source=source,
                format_hint=format_hint,
            )

            if resource is not None:
                resources.append(resource)

        return resources

    def lookup_rcsbpdb(
        self,
        pdb_id: Optional[str],
        original_identifier: str,
        doi: Optional[str] = None,
        query_source: Optional[str] = None,
    ) -> RCSBPDBResolution:
        result = RCSBPDBResolution(
            original_identifier=original_identifier,
            normalized_identifier=doi or pdb_id or str(original_identifier),
            repo="rcsbpdb",
            endpoint_used=self.ENDPOINTS["entry"],
            endpoint_variables={"pdb_id": pdb_id},
            doi=doi,
            query_source=query_source,
        )

        if not pdb_id:
            result.notes.append("Could not resolve a 4-character PDB ID.")
            return result

        meta_url = self.ENDPOINTS["entry"].format(pdb_id=pdb_id)
        landing_url = self.ENDPOINTS["entry_landing"].format(pdb_id=pdb_id)

        result.metadata_url = meta_url
        result.landing_page_url = landing_url
        
        try:
            meta = self._request(meta_url)

            result.status = "ok"
            result.title = meta.get("struct", {}).get("title")
            result.record_id = pdb_id

            if result.doi is None:
                result.doi = f"10.2210/pdb{pdb_id.lower()}/pdb"

            result.raw_metadata = {
                "entry": pdb_id,
                "title": result.title,
                "experimental_methods": meta.get("exptl", []),
                "keywords": meta.get("struct_keywords", {}),
                "rcsb_accession_info": meta.get("rcsb_accession_info", {}),
                "citation": meta.get("citation", []),
                "primary_citation": meta.get("rcsb_primary_citation", {}),
                "full_metadata": meta,
            }

            result.files = self._build_file_resources(pdb_id, meta)

            return result

        except requests.HTTPError as exc:
            result.status = f"http_error_{getattr(exc.response, 'status_code', 'unknown')}"
            result.notes.append("RCSB entry metadata request failed.")
            return result

        except requests.RequestException as exc:
            result.status = "request_failed"
            result.notes.append(f"rcsbpdb request error: {str(exc)}")
            return result

        except Exception as exc:
            result.status = "parse_failed"
            result.notes.append(f"rcsbpdb parsing error: {str(exc)}")
            return result

    # ------------------------------------------------------------------
    # Loading and normalization
    # ------------------------------------------------------------------
    def _load_initial_data(self) -> None:
        self.raw_results = [
            self.lookup_identifier(identifier, query_source="identifier")
            for identifier in self.identifiers
        ]
        self.process_artifacts()

    def _extract_tables(self, results: List[RCSBPDBResolution]) -> Dict[str, List[Dict[str, Any]]]:
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
                    "source_repository": "RCSBPDB",
                    "doi": (
                        res.doi
                        or self._extract_doi(res.raw_metadata)
                        or (f"10.2210/pdb{str(res.record_id).lower()}/pdb" if res.record_id else None)
                    ),
                    "title": res.title,
                    "description": self._extract_description(res.raw_metadata),
                    "landing_page": res.landing_page_url,
                    "metadata_url": res.metadata_url,
                    "experimental_method": self._extract_experimental_method(res.raw_metadata),
                    "release_date": self._extract_release_date(res.raw_metadata),
                    "revision_date": self._extract_revision_date(res.raw_metadata),
                    "resource_count": len(res.files),
                    "usability_label": self.classify_usability(file_exts),
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
                        "source_repository": "RCSBPDB",
                        "name": file_obj.label,
                        "download_url": file_obj.url,
                        "format": file_obj.extension,
                        "resource_type": file_obj.format_hint,
                        "source": file_obj.source,
                        "raw_metadata": asdict(file_obj),
                    }
                )

        return {
            "datasets": self._apply_schema(datasets, self.DATASET_SCHEMA),
            "resources": self._apply_schema(resources, self.RESOURCE_SCHEMA),
            "errors": self._apply_schema(errors, self.ERROR_SCHEMA),
        }

    def _apply_schema(self, rows: List[Dict[str, Any]], schema: List[str]) -> List[Dict[str, Any]]:
        return [{column: row.get(column) for column in schema} for row in rows]

    def _rows_to_table(self, rows: List[Dict[str, Any]]):
        table = OrderedDict()

        if not rows:
            return table

        for column in rows[0].keys():
            table[column] = [row.get(column) for row in rows]

        return table

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

    @staticmethod
    def _extract_doi(raw_metadata: Dict[str, Any]) -> Optional[str]:
        full_metadata = raw_metadata.get("full_metadata", {})

        citation = full_metadata.get("rcsb_primary_citation", {})
        if isinstance(citation, dict):
            doi = citation.get("pdbx_database_id_DOI")
            if doi:
                return doi

        citations = raw_metadata.get("citation", [])
        if isinstance(citations, list):
            for item in citations:
                if isinstance(item, dict):
                    doi = item.get("pdbx_database_id_DOI")
                    if doi:
                        return doi

        return None

    @staticmethod
    def _extract_description(raw_metadata: Dict[str, Any]) -> Optional[str]:
        full_metadata = raw_metadata.get("full_metadata", {})

        struct_keywords = full_metadata.get("struct_keywords", {})
        if isinstance(struct_keywords, dict):
            keywords = struct_keywords.get("pdbx_keywords")
            text = struct_keywords.get("text")

            if keywords and text:
                return f"{keywords}: {text}"
            if text:
                return text
            if keywords:
                return keywords

        return None

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------
    def get_table(self, table_name: str, dict_return=False):
        resolved = self._resolve_table_name(table_name)
        table = self.tables.get(resolved, OrderedDict())

        if dict_return:
            return table

        if not table:
            return pd.DataFrame(columns=self.get_schema(resolved))

        return pd.DataFrame(table)

    def get_tables(self):
        return self.tables

    def get_schema(self, table_name: Optional[str] = None):
        if table_name is None:
            return self.schemas
        resolved = self._resolve_table_name(table_name)
        return self.schemas.get(resolved, [])

    def get_table_names(self) -> List[str]:
        return list(self.tables.keys())

    def num_tables(self):
        table_count = len(self.tables)
        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")

    def overwrite_table(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        resolved = self._resolve_table_name(table_name)
        schema = self.schemas.get(resolved, list(rows[0].keys()) if rows else [])
        self.tables[resolved] = self._rows_to_table(self._apply_schema(rows, schema))
        self.schemas[resolved] = schema

    def validate_urls(self, table_name: str = "resources", url_column: str = "download_url", **kwargs):
        rows = self.get_table(table_name)
        results = []

        for idx, row in rows.iterrows():
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
        raise NotImplementedError("rcsbpdb backend is read-only.")

    def query_artifacts(self, query, **kwargs):
        raise NotImplementedError(
            "query_artifacts() is not implemented for RCSBPDB because it is not an SQL backend. "
            "Use find_relation() for RCSBPDB API-backed lookup/search behavior."
        )

    def notebook(self, **kwargs):
        """
        Notebook generation is not supported for the rcsbpdb backend.
        """
        raise NotImplementedError(
            "Notebook generation is not supported for the rcsbpdb backend."
        )

    def process_artifacts(self, **kwargs):
        extracted = self._extract_tables(self.raw_results)
        self.tables = {}

        for table_name, rows in extracted.items():
            self.tables[table_name] = self._rows_to_table(rows)
            self.schemas[table_name] = (
                list(rows[0].keys()) if rows else self.schemas.get(table_name, [])
            )

        self._loaded = True
        return self.tables

    def find(self, query_object, **kwargs):
        needle = str(query_object).lower()
        matches: List[ValueObject] = []

        for table_name, table in self.tables.items():
            if not table:
                continue

            columns = list(table.keys())
            rows = zip(*table.values())

            for row_num, row in enumerate(rows):
                for col_name, value in zip(columns, row):
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

    def find_relation(self, query, relation=None, **kwargs):
        if query is None:
            return self.tables

        try:
            if isinstance(query, dict):
                self.params = query
                self._load_from_params(query)
                self._loaded = True
                return self.tables

            if isinstance(query, str):
                kind = self.classify_identifier(query)

                if kind in {"rcsbpdb_doi", "pdb_id"}:
                    self.identifiers = [query]
                    self._load_initial_data()
                else:
                    query_params = {"keywords": query}
                    query_params.update(kwargs)
                    self.params = query_params
                    self._load_from_params(self.params)

                self._loaded = True
                return self.tables

            if isinstance(query, list):
                self.identifiers = query
                self._load_initial_data()
                self._loaded = True
                return self.tables

        except Exception:
            self._loaded = False
            raise

        raise TypeError("find_relation() expects None, str, list, or dict.")

    def list(self, collection=False, **kwargs):
        table_names = self.get_table_names()

        if collection:
            return table_names

        for table_name in table_names:
            df = self.get_table(table_name)
            print(f"\nTable: {table_name}")
            print(f"  - num of columns: {df.shape[1]}")
            print(f"  - num of rows: {df.shape[0]}")
        print()

    def display(self, table_name=None, **kwargs):
        if table_name is None:
            return self.tables
        return self.get_table(table_name)

    def summary(self, table_name=None, *args, **kwargs):
        """
        Returns summary metadata for RCSBPDB tables.

        If table_name is provided:
            Returns a single DataFrame.

        If table_name is None:
            Returns [table_names, df1, df2, ...]
            which matches the format expected by DSI.
        """

        def summarize_table(name):
            resolved = self._resolve_table_name(name)
            df = self.get_table(resolved)

            summary_dict = {
                "table_name": resolved,
                "num_rows": df.shape[0],
                "num_columns": df.shape[1],
                "columns": list(df.columns),
            }

            if resolved == "datasets":
                summary_dict["tier"] = "Tier 1"
                summary_dict["description"] = (
                    "Dataset-level RCSBPDB metadata"
                )

            elif resolved == "resources":
                summary_dict["tier"] = "Tier 2"
                summary_dict["description"] = (
                    "Resource-level file metadata and download paths"
                )
                summary_dict["foreign_key"] = "dataset_id"
                summary_dict["references"] = "datasets.dataset_id"

            elif resolved == "errors":
                summary_dict["tier"] = "Errors"
                summary_dict["description"] = (
                    "Failed, skipped, or unresolved lookups"
                )

            return pd.DataFrame([summary_dict])

        # Single-table summary
        if table_name is not None:
            return summarize_table(table_name)

        # All-table summary
        table_names = self.get_table_names()

        summary_dfs = [
            summarize_table(name)
            for name in table_names
        ]

        return [table_names] + summary_dfs

    def close(self):
        """
        Resets backend state, clears loaded rcsbpdb data, and releases HTTP resources.
        """
        if hasattr(self, "session"):
            self.session.close()

        self.tables = {}
        self.raw_results = []
        self.last_search_response = None
        self.identifiers = []
        self.params = {}
        self._loaded = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        