"""
RCSBPDB Webserver Backend for DSI

Read-only metadata-first backend that retrieves rcsb pdb metadata
and exposes it as in-memory DSI tables.
"""
# Access modes
# ------------
# 1. Identifier-driven mode
#    - DOI input, e.g. "10.2210/pdb1cbs/pdb"
#    - PDB ID input, e.g. "1CBS"

# 2. Query-driven mode, closer to NDP
#    - params={"keywords": "hemoglobin", "limit": 5}
#    - find_relation({"keywords": "hemoglobin", "limit": 5})
#    - find_relation("hemoglobin")

# DOI behavior
# ------------
# RCSB-style DOI input is supported through identifiers or params.
# Only DOIs of the form 10.2210/pdbXXXX/pdb are converted directly into PDB IDs.
# General publication DOI search is not currently supported.

# REST flow
# ---------
# RCSB Search API -> PDB IDs -> RCSB Data API -> normalized DSI tables

# Tables
# ------
# - datasets
# - resources
# - errors

# Tier mapping
# ------------
# self.tables["datasets"] = Tier 1 datasets
# self.tables["resources"] = Tier 2 normalized resources
# self.tables["errors"] = failed/skipped lookups

# Current scope
# -------------
# - Metadata-first
# - Read-only
# - REST APIs only
# - Exposes mmCIF download URLs
# - Does not parse raw mmCIF content yet

# identifiers
# → __init__()
# → _load_initial_data()
# → lookup_identifier()
# → lookup_rcsbpdb()
# → _request()
# → GET https://data.rcsb.org/rest/v1/core/entry/{pdb_id}

# params
# → __init__()
# → _load_from_params()
# → _search_rcsb()
# → _build_search_query()
# → _post_json()
# → POST https://search.rcsb.org/rcsbsearch/v2/query


from __future__ import annotations

import builtins
import json
import re
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar

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

    def to_dict(self) -> dict[str, Any]:
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

    label: str | None
    url: str
    extension: str | None
    source: str
    format_hint: str | None = None
    exists: bool | None = None
    status_code: int | None = None
    content_type: str | None = None


@dataclass
class RCSBPDBResolution:
    """Internal normalized result object for one rcsbpdb/RCSB lookup."""

    original_identifier: str
    normalized_identifier: str
    repo: str
    endpoint_used: str | None
    endpoint_variables: dict[str, Any] = field(default_factory=dict)
    status: str = "no_match"
    title: str | None = None
    record_id: str | None = None
    doi: str | None = None
    metadata_url: str | None = None
    landing_page_url: str | None = None
    query_source: str | None = None
    files: list[FileResource] = field(default_factory=list)
    raw_metadata: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


class RCSBPDB(Webserver):
    """
    rcsbpdb/RCSB metadata backend for DSI.

    Implements the DSI Webserver interface and exposes RCSB/rcsbpdb
    metadata as in-memory DSI tables.
    """
    read_only = True
    DATA_CORE_URL = "https://data.rcsb.org/rest/v1/core"
    SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"

    ENDPOINTS: ClassVar[dict[str, str]] = {
        "search": SEARCH_URL,
        "data_core": DATA_CORE_URL,
        "entry": f"{DATA_CORE_URL}/entry/{{pdb_id}}",
        "entry_landing": "https://www.rcsb.org/structure/{pdb_id}",
        "mmcif_gz": (
            "https://files.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/"
            "{subdir}/{pdb_id}.cif.gz"
        ),
    }

    DOI_REGEX = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
    RCSBPDB_DOI_REGEX = re.compile(r"10\.2210/pdb([a-z0-9]{4})/pdb", re.IGNORECASE)
    PDB_ID_REGEX = re.compile(r"^[A-Za-z0-9]{4}$")

    DATASET_SCHEMA: ClassVar[list[str]] = [
        "dataset_id",
        "doi",
        "title",
        "description",
        "landing_page",
        "metadata_url",
        "experimental_method",
        "release_date",
        "revision_date",
        "resource_count",
        "raw_metadata",
        "notes",
    ]

    RESOURCE_SCHEMA: ClassVar[list[str]] = [
        "resource_id",
        "dataset_id",
        "name",
        "download_url",
        "format",
        "resource_type",
        "source",
        "raw_metadata",
    ]

    ERROR_SCHEMA: ClassVar[list[str]] = [
        "identifier",
        "normalized_identifier",
        "repo",
        "status",
        "endpoint_used",
        "endpoint_variables",
        "notes",
    ]

    SUPPORTED_PARAMS: ClassVar[set[str]] = {
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
        url: str | None = None,
        identifiers: builtins.list[str] | None = None,
        params: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize rcsbpdb backend and optionally load data from RCSB APIs.
        """
        self.url = (url or self.DATA_CORE_URL).rstrip("/")
        self.identifiers = list(dict.fromkeys(identifiers or []))
        self.params = params or {}
        self.validate_error_msg = None

        # skip data retrieval if only checking connection to rcsbpdb
        if kwargs.get("only_validate", False):
            return

        self.timeout = kwargs.get("timeout", 60)
        self.verify = kwargs.get("verify_ssl", kwargs.get("verify", True))
        self.validate_resource_urls = kwargs.get("validate_resource_urls", True)
        self.retries = kwargs.get("retries", 3)
        self.validate_on_init = kwargs.get("validate_on_init", True)
        self.auto_load = kwargs.get("auto_load", True)
        self.kwargs = kwargs

        self.session = self._create_session(retries=self.retries)

        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.schemas: dict[str, list[str]] = {
            "datasets": self.DATASET_SCHEMA,
            "resources": self.RESOURCE_SCHEMA,
            "errors": self.ERROR_SCHEMA,
        }

        self.raw_results: list[RCSBPDBResolution] = []
        self.last_search_response: dict[str, Any] | None = None
        self._loaded = False

        if self.validate_on_init and not self.validate_connection():
            self._loaded = False
            raise ConnectionError(self.validate_error_msg or "Validating RCSBPDB connection failed.")

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
        """
        Validate that the RCSB Data API is reachable and responsive.

        Return
        ------
        bool
            True if connection is valid, False otherwise.
        """
        try:
            test_url = self.ENDPOINTS["entry"].format(pdb_id="1CBS")

            response = self.session.get(
                test_url,
                timeout=self.timeout,
                verify=self.verify,
            )
            response.raise_for_status()
            return True

        except Exception as e:
            self.validate_error_msg = f"Unable to connect to RCSB Data API: {e}"
            return False

    def _request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
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

    def _post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
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
    def normalize_doi(cls, value: Any) -> str | None:
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
    def normalize_pdb_id(cls, value: Any) -> str | None:
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
    def extract_pdb_id_from_doi(cls, doi: str) -> str | None:
        match = cls.RCSBPDB_DOI_REGEX.search(doi)
        return match.group(1).upper() if match else None

    @staticmethod
    def get_file_ext(name_or_url: str | None) -> str | None:
        if not isinstance(name_or_url, str) or "." not in name_or_url:
            return None

        tail = name_or_url.split("?")[0].split("/")[-1].lower()
        parts = tail.split(".")

        if len(parts) >= 3 and parts[-1] == "gz":
            return ".".join(parts[-2:])

        return parts[-1]

    @staticmethod
    def classify_usability(exts: Iterable[str | None]) -> str:
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
    def _extract_identifiers_from_params(self, params: dict[str, Any]) -> builtins.list[str]:
        """
        Extract DOI/PDB identifiers from params and normalize aliases.

        Supported identifier-style params:
        - identifiers
        - pdb_id, pdbID, pdbId, PDB_ID, pdbid, PDBID
        - doi, DOI
        """
        identifiers: list[str] = []

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
    
    def _load_from_params(self, params: dict[str, Any]) -> None:
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

    def _search_rcsb(self, params: dict[str, Any]) -> builtins.list[str]:
        """
        Search RCSB and return PDB IDs.
        """
        self._validate_params(params)

        limit = int(params.get("limit", 10))
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

    def _validate_params(self, params: dict[str, Any]) -> None:
        unsupported = set(params.keys()) - self.SUPPORTED_PARAMS
        if unsupported:
            raise ValueError(
                f"Unsupported rcsbpdb search params: {sorted(unsupported)}. "
                f"Supported params: {sorted(self.SUPPORTED_PARAMS)}"
            )

    def _build_search_query(self, params: dict[str, Any]) -> dict[str, Any] | None:
        nodes: list[dict[str, Any]] = []

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
        query_source: str | None = None,
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
    ) -> FileResource | None:
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

    def _build_file_resources(self, pdb_id: str, meta: dict[str, Any]) -> builtins.list[FileResource]:
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
        pdb_id: str | None,
        original_identifier: str,
        doi: str | None = None,
        query_source: str | None = None,
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
            result.notes.append(f"rcsbpdb request error: {exc!s}")
            return result

        except Exception as exc:
            result.status = "parse_failed"
            result.notes.append(f"rcsbpdb parsing error: {exc!s}")
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

    def _extract_tables(self, results: builtins.list[RCSBPDBResolution]) -> dict[str, builtins.list[dict[str, Any]]]:
        datasets: list[dict[str, Any]] = []
        resources: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for res in results:
            if res.status != "ok":
                errors.append(
                    {
                        "identifier": res.original_identifier,
                        "normalized_identifier": res.normalized_identifier,
                        "repo": res.repo,
                        "status": res.status,
                        "endpoint_used": res.endpoint_used,
                        "endpoint_variables": self._json_or_none(res.endpoint_variables),
                        "notes": " | ".join(res.notes) if res.notes else None,
                    }
                )
                continue

            datasets.append(
                {
                    "dataset_id": res.record_id,
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
                    "raw_metadata": self._json_or_none(res.raw_metadata),
                    "notes": " | ".join(res.notes) if res.notes else None,
                }
            )

            for idx, file_obj in enumerate(res.files, start=1):
                resources.append(
                    {
                        "resource_id": f"{res.record_id}:{idx}",
                        "dataset_id": res.record_id,
                        "name": file_obj.label,
                        "download_url": file_obj.url,
                        "format": file_obj.extension,
                        "resource_type": file_obj.format_hint,
                        "source": file_obj.source,
                        "raw_metadata": self._json_or_none(asdict(file_obj)),
                    }
                )

        return {
            "datasets": self._apply_schema(datasets, self.DATASET_SCHEMA),
            "resources": self._apply_schema(resources, self.RESOURCE_SCHEMA),
            "errors": self._apply_schema(errors, self.ERROR_SCHEMA),
        }

    def _apply_schema(self, rows: builtins.list[dict[str, Any]], schema: builtins.list[str]) -> builtins.list[dict[str, Any]]:
        return [{column: row.get(column) for column in schema} for row in rows]

    def _rows_to_table(self, rows: builtins.list[dict[str, Any]], schema: builtins.list[str]):
        table = OrderedDict()

        for column in schema:
            table[column] = [row.get(column) for row in rows]

        return table

    def _resolve_table_name(self, table_name: str | None) -> str | None:
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
    def _json_or_none(value):
        if value is None:
            return None

        try:
            return json.dumps(value, sort_keys=True)
        except TypeError:
            return str(value)

    @staticmethod
    def _extract_experimental_method(raw_metadata: dict[str, Any]) -> str | None:
        methods = raw_metadata.get("experimental_methods", [])
        extracted = []

        for entry in methods:
            if isinstance(entry, dict) and entry.get("method"):
                extracted.append(entry["method"])

        return " | ".join(extracted) if extracted else None

    @staticmethod
    def _extract_release_date(raw_metadata: dict[str, Any]) -> str | None:
        accession_info = raw_metadata.get("rcsb_accession_info", {})
        return accession_info.get("initial_release_date") if isinstance(accession_info, dict) else None

    @staticmethod
    def _extract_revision_date(raw_metadata: dict[str, Any]) -> str | None:
        accession_info = raw_metadata.get("rcsb_accession_info", {})
        return accession_info.get("revision_date") if isinstance(accession_info, dict) else None

    @staticmethod
    def _extract_doi(raw_metadata: dict[str, Any]) -> str | None:
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
    def _extract_description(raw_metadata: dict[str, Any]) -> str | None:
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

    def _parse_find_query(self, query: str):
        operators = ["~~", ">=", "<=", "!=", "==", ">", "<", "=", "~"]

        for op in operators:
            pattern = rf"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*{re.escape(op)}\s*(.+?)\s*$"
            match = re.match(pattern, query)

            if match:
                column_name = match.group(1)
                value = match.group(2).strip().strip("'").strip('"')
                return column_name, op, value

        return None

    def _apply_pandas_filter(self, df, column, operator, value):
        """Apply comparison filter using pandas."""
        if operator == ">":
            return df[df[column] > value]
        if operator == "<":
            return df[df[column] < value]
        if operator == ">=":
            return df[df[column] >= value]
        if operator == "<=":
            return df[df[column] <= value]
        if operator == "==":
            return df[df[column] == value]
        if operator == "!=":
            return df[df[column] != value]
        if operator == "contains":
            return df[
                df[column].astype(str).str.contains(
                    str(value),
                    case=False,
                    na=False,
                )
            ]
        if operator == "range":
            min_val, max_val = value
            return df[(df[column] >= min_val) & (df[column] <= max_val)]

        return pd.DataFrame()


    def _parse_relation(self, relation):
        """
        Parse relation string into operator and value.

        Examples:
        '> 5' -> ('>', 5)
        '<= 10' -> ('<=', 10)
        '== 3' -> ('==', 3)
        "(2, 5)" -> ('range', (2, 5))
        "~~ 'climate'" -> ('contains', 'climate')
        """
        relation = relation.strip()

        if relation.startswith(">="):
            return ">=", self._parse_value(relation[2:])
        if relation.startswith("<="):
            return "<=", self._parse_value(relation[2:])
        if relation.startswith("=="):
            return "==", self._parse_value(relation[2:])
        if relation.startswith("!="):
            return "!=", self._parse_value(relation[2:])
        if relation.startswith("~~"):
            return "contains", self._parse_value(relation[2:])

        if relation.startswith(">"):
            return ">", self._parse_value(relation[1:])
        if relation.startswith("<"):
            return "<", self._parse_value(relation[1:])
        if relation.startswith("="):
            return "==", self._parse_value(relation[1:])
        if relation.startswith("~"):
            return "contains", self._parse_value(relation[1:])

        if relation.startswith("(") and relation.endswith(")"):
            parts = relation[1:-1].split(",")
            if len(parts) == 2:
                return "range", (
                    self._parse_value(parts[0]),
                    self._parse_value(parts[1]),
                )

        raise ValueError(f"Unknown relation format: {relation}")


    def _parse_value(self, value_str):
        """Convert string to appropriate Python type."""
        value_str = str(value_str).strip()

        if (
            value_str.startswith("'")
            and value_str.endswith("'")
        ) or (
            value_str.startswith('"')
            and value_str.endswith('"')
        ):
            value_str = value_str[1:-1]

        try:
            if "." not in value_str:
                return int(value_str)
            return float(value_str)
        except ValueError:
            return value_str

    def get_tables(self):
        return self.tables

    def get_schema(self, table_name: str | None = None):
        if table_name is None:
            return self.schemas
        resolved = self._resolve_table_name(table_name)
        return self.schemas.get(resolved, [])

    def num_tables(self):
        table_count = len(self.tables)

        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")

        return table_count


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
        raise NotImplementedError("query() is not implemented for RCSBPDB because it is not a SQL backend.")

    def notebook(self, **kwargs):
        """
        Notebook generation is not supported for the rcsbpdb backend.
        """
        raise NotImplementedError("Notebook generation is not supported for the rcsbpdb backend.")

    def process_artifacts(self, **kwargs):
        extracted = self._extract_tables(self.raw_results)
        self.tables = OrderedDict()

        for table_name, rows in extracted.items():
            schema = self.schemas[table_name]
            table = self._rows_to_table(rows, schema)

            self.tables[table_name] = table
            self.schemas[table_name] = schema

        self._loaded = True
        return self.tables

    def find(self, query_object, **kwargs):
        """
        Searches for all instances of query_object across all loaded tables.

        Searches at the table, column, and cell levels.
        """
        query_str = str(query_object).lower()

        return (
            self.find_table(query_str)
            + self.find_column(query_str)
            + self.find_cell(query_object)
        )

    def find_table(self, query_object, **kwargs):
        if not isinstance(query_object, str):
            raise TypeError("find_table() ERROR: query_object must be a string")

        matches = []

        for table_name, table_data in self.tables.items():
            if query_object in table_name.lower():
                val = ValueObject()
                val.t_name = table_name
                val.c_name = list(table_data.keys())
                val.value = table_data
                val.type = "table"

                matches.append(val)

        return matches

    def find_column(self, query_object, **kwargs):
        if not isinstance(query_object, str):
            raise TypeError("find_column() ERROR: query_object must be a string")

        matches = []

        for table_name, table_data in self.tables.items():
            for col_name, col_data in table_data.items():
                if query_object in col_name.lower():
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    val.value = col_data
                    val.type = "column"

                    matches.append(val)

        return matches

    def find_cell(self, query_object, **kwargs):
        """
        Finds all cells that match the given query_object.

        Matching behavior:
        - Exact match for all data types
        - Case-insensitive partial match for strings
        - String representation match for complex objects
        """
        matches = []

        is_str_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str_query else None

        for table_name, table_data in self.tables.items():
            if not table_data:
                continue

            cols = list(table_data.keys())
            df = pd.DataFrame(table_data)

            for row_idx, row in df.iterrows():
                for col in cols:
                    cell = row[col]
                    match = False

                    if (
                        (pd.isna(cell) and pd.isna(query_object))
                        or query_object == cell
                        or (
                            is_str_query
                            and isinstance(cell, str)
                            and query_lower in cell.lower()
                        )
                    ):
                        match = True

                    elif is_str_query and isinstance(cell, (dict, list, tuple)):
                        cell_str = str(cell).lower()
                        if query_lower in cell_str:
                            match = True

                    if match:
                        val = ValueObject()
                        val.t_name = table_name
                        val.c_name = cols
                        val.row_num = row_idx
                        val.value = row.tolist()
                        val.type = "cell"

                        matches.append(val)

        return matches

    def find_relation(self, query, relation=None, **kwargs):
        """
        Supports both NDP-style condition queries and RCSBPDB API-backed lookup/search.

        Examples:
            find_relation("resource_count", "> 8")
            find_relation("dataset_id", "= 1CBS")
            find_relation("title", "~~ structure")
            find_relation("resource_count > 8")
            find_relation("1CBS")
            find_relation("10.2210/pdb1cbs/pdb")
            find_relation("hemoglobin", limit=2)
            find_relation({"keywords": "hemoglobin", "limit": 2})
            find_relation(["1CBS", "4HHB"])
        """
        if query is None:
            return self.tables

        try:
            # find_relation("dataset_id", "= 1CBS")
            if relation is not None:
                operator, value = self._parse_relation(relation)
                matches = []

                for table_name in self.list(collection=True):
                    df = self.get_table(table_name, dict_return=False)

                    if df.empty or query not in df.columns:
                        continue

                    if operator in {">", "<", ">=", "<=", "range"}:
                        df[query] = pd.to_numeric(df[query], errors="coerce")
                        df = df.dropna(subset=[query])

                    filtered = self._apply_pandas_filter(df, query, operator, value)

                    for idx, row in filtered.iterrows():
                        vo = ValueObject()
                        vo.t_name = table_name
                        vo.c_name = list(df.columns)
                        vo.row_num = int(idx) + 1
                        vo.value = row.tolist()
                        vo.type = "cell"
                        matches.append(vo)

                return matches

            # One-string condition:
            # find_relation("dataset_id = 1CBS")
            if isinstance(query, str):
                parsed = self._parse_find_query(query)

                if parsed is not None:
                    column_name, op, target_value = parsed
                    operator, value = self._parse_relation(f"{op} {target_value}")
                    matches = []

                    for table_name in self.list(collection=True):
                        df = self.get_table(table_name, dict_return=False)

                        if df.empty or column_name not in df.columns:
                            continue

                        if operator in {">", "<", ">=", "<=", "range"}:
                            df[column_name] = pd.to_numeric(
                                df[column_name],
                                errors="coerce",
                            )
                            df = df.dropna(subset=[column_name])

                        filtered = self._apply_pandas_filter(
                            df,
                            column_name,
                            operator,
                            value,
                        )

                        for idx, row in filtered.iterrows():
                            vo = ValueObject()
                            vo.t_name = table_name
                            vo.c_name = list(df.columns)
                            vo.row_num = int(idx) + 1
                            vo.value = row.tolist()
                            vo.type = "cell"
                            matches.append(vo)

                    return matches

            # API-backed query path:
            # find_relation({"keywords": "hemoglobin", "limit": 2})
            if isinstance(query, dict):
                self.params = query
                self._load_from_params(query)
                self._loaded = True
                return self.tables

            # API-backed list path:
            # find_relation(["1CBS", "4HHB"])
            if isinstance(query, list):
                self.identifiers = query
                self._load_initial_data()
                self._loaded = True
                return self.tables

            # API-backed string path:
            # find_relation("1CBS")
            # find_relation("10.2210/pdb1cbs/pdb")
            # find_relation("hemoglobin", limit=2)
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

        except Exception:
            self._loaded = False
            raise

        raise TypeError("find_relation() expects None, str, list, or dict.")
    
    def list(self, collection=False, **kwargs):
        table_names = list(self.tables.keys())

        if collection:
            return table_names

        for table_name in table_names:
            df = self.get_table(table_name)
            print(f"\nTable: {table_name}")
            print(f"  - num of columns: {df.shape[1]}")
            print(f"  - num of rows: {df.shape[0]}")
        print()

    def display(self, table_name=None, num_rows=25, display_cols=None, **kwargs):
        """
        Print data from a table.

        Parameters
        ----------
        table_name : str
            Name of the table to display.
        num_rows : int, optional
            Number of rows to print.
        display_cols : list[str], optional
            Specific columns to display.
        """
        if table_name is None:
            raise ValueError(
                "display() requires a table_name. "
                f"Available tables: {self.list(True)}"
            )

        resolved = self._resolve_table_name(table_name)

        if resolved not in self.schemas:
            raise ValueError(
                f"display() could not find table '{table_name}'. "
                f"Available tables: {self.list(True)}"
            )

        df = self.get_table(resolved)

        if df.empty:
            raise ValueError(f"Table '{resolved}' is empty.")

        if display_cols is not None:
            missing_cols = [col for col in display_cols if col not in df.columns]

            if missing_cols:
                raise ValueError(
                    f"display() could not find column(s) {missing_cols} "
                    f"in table '{resolved}'. Available columns: {list(df.columns)}"
                )

            df = df[display_cols]

        print(df.head(num_rows).to_string(index=False))


    def summary(self, table_name=None, *args, **kwargs):
        def is_complex_value(value):
            return isinstance(value, (dict, list, tuple, set))

        def is_url_or_metadata_column(column):
            return column in {
                "raw_metadata",
                "landing_page",
                "metadata_url",
                "download_url",
                "endpoint_variables",
            }

        def is_long_text_series(series):
            non_null = series.dropna()

            if non_null.empty:
                return False

            string_series = non_null.astype(str)
            return string_series.str.len().max() > 80

        def summarize_dataframe(df):
            rows = []

            for column in df.columns:
                original_series = df[column]
                non_null = original_series.dropna()

                has_complex_values = (
                    non_null.apply(is_complex_value).any()
                    if not non_null.empty
                    else False
                )

                safe_series = non_null.astype(str) if has_complex_values else non_null

                dtype = str(original_series.dtype).upper()
                unique = int(safe_series.nunique()) if not safe_series.empty else 0

                row = {
                    "column": column,
                    "type": dtype,
                    "unique": unique,
                    "min": None,
                    "max": None,
                    "avg": None,
                    "std_dev": None,
                }

                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()

                if not non_null.empty and len(numeric_series) == len(non_null):
                    row["min"] = numeric_series.min()
                    row["max"] = numeric_series.max()
                    row["avg"] = numeric_series.mean()
                    row["std_dev"] = numeric_series.std()

                elif (
                    not non_null.empty
                    and not has_complex_values
                    and not is_url_or_metadata_column(column)
                    and not is_long_text_series(non_null)
                ):
                    try:
                        row["min"] = non_null.min()
                        row["max"] = non_null.max()
                    except TypeError:
                        row["min"] = None
                        row["max"] = None

                rows.append(row)

            return pd.DataFrame(
                rows,
                columns=[
                    "column",
                    "type",
                    "unique",
                    "min",
                    "max",
                    "avg",
                    "std_dev",
                ],
            )

        if table_name is not None:
            resolved = self._resolve_table_name(table_name)
            df = self.get_table(resolved)
            return summarize_dataframe(df)

        table_names = self.list(True)
        summary_tables = []

        for name in table_names:
            df = self.get_table(name)
            summary_tables.append(summarize_dataframe(df))

        return [table_names] + summary_tables

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
        