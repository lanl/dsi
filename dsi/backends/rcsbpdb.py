"""RCSBPDB Webserver Backend for DSI.

Read-only, metadata-first access to public RCSB Protein Data Bank records.

The backend opens and validates an API connection at initialization. Users then
retrieve remote data explicitly through ``query_artifacts()``. Dictionary
queries return exactly one pandas DataFrame and append its ordered table to the
in-memory cache. String queries explore tables that are already cached.
"""


from __future__ import annotations

import builtins
import json
import re
import warnings
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dsi.backends.webserver import Webserver


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
    resources_loaded: bool = False
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
    REQUEST_TIMEOUT = 60

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
        **kwargs,
    ) -> None:
        """Open and validate an RCSB connection with an empty cache."""
        if kwargs.get("identifiers") or kwargs.get("params"):
            raise ValueError(
                "RCSBPDB initialization does not retrieve artifacts. "
                "Create the backend first, then call query_artifacts({...})."
            )

        if "url" in kwargs:
            raise ValueError("RCSBPDB uses the fixed RCSB API endpoints; url is not supported.")

        self.validate_error_msg = None
        self.verify = kwargs.get("verify", True)
        self.session = self._create_session(retries=kwargs.get("retries", 3))

        # Each cached table is an ordered mapping of column names to values.
        self._cache: OrderedDict[str, OrderedDict[str, list[Any]]] = OrderedDict()

        self._loaded = False

        # list_backends() uses only_validate=True and validates separately.
        # Session and cache setup must still be available in that case.
        if kwargs.get("only_validate", False):
            self._loaded = True
            return

        if kwargs.get("validate_on_init", True) and not self.validate_connection():
            raise ConnectionError(
                self.validate_error_msg or "Validating RCSBPDB connection failed."
            )

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
                timeout=self.REQUEST_TIMEOUT,
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
            timeout=self.REQUEST_TIMEOUT,
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
            timeout=self.REQUEST_TIMEOUT,
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
    
    @staticmethod
    def _cache_token(value: Any) -> str:
        """Create a readable token that is safe to use in a cache-table name."""
        token = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
        return token[:60] or "query"

    def _discovery_table_name(self, params: dict[str, Any]) -> str:
        for key in ("keywords", "authors", "experimental_method"):
            if params.get(key):
                return f"pdb_datasets_{self._cache_token(params[key])}"
        return "pdb_datasets_query"

    def _resource_table_name(self, results: list[RCSBPDBResolution]) -> str:
        pdb_ids = [
            str(result.record_id).lower()
            for result in results
            if result.record_id
        ]
        return f"pdb_resources_{'_'.join(pdb_ids)}" if pdb_ids else "pdb_resources_query"

    def _query_discovery(
        self,
        params: dict[str, Any],
    ) -> tuple[str, list[dict[str, Any]], list[str]]:
        """Tier 1: return high-level metadata for matching PDB entries."""
        pdb_ids = self._search_rcsb(params)
        results = [
            self.lookup_identifier(
                pdb_id,
                query_source="query_artifacts.discovery",
                include_resources=False,
            )
            for pdb_id in pdb_ids
        ]

        extracted = self._extract_tables(results)
        table_name = self._discovery_table_name(params)

        if extracted["datasets"] or not extracted["errors"]:
            if extracted["datasets"] and extracted["errors"]:
                warnings.warn(
                    "Some PDB entries could not be retrieved: "
                    + ", ".join(str(error["identifier"]) for error in extracted["errors"])
                    + ". Query each identifier separately to inspect its error.",
                    UserWarning,
                    stacklevel=2,
                )
            return table_name, extracted["datasets"], self.DATASET_SCHEMA

        return f"{table_name}_errors", extracted["errors"], self.ERROR_SCHEMA

    def _query_selected_entries(
        self,
        identifiers: list[str],
    ) -> tuple[str, list[dict[str, Any]], list[str]]:
        """Tier 2: return resource/file pointers for selected PDB entries."""
        results = [
            self.lookup_identifier(
                identifier,
                query_source="query_artifacts.selected_entry",
                include_resources=True,
            )
            for identifier in identifiers
        ]

        extracted = self._extract_tables(results)
        table_name = self._resource_table_name(results)

        if extracted["resources"] or not extracted["errors"]:
            if extracted["resources"] and extracted["errors"]:
                warnings.warn(
                    "Some PDB entries could not be retrieved: "
                    + ", ".join(str(error["identifier"]) for error in extracted["errors"])
                    + ". Query each identifier separately to inspect its error.",
                    UserWarning,
                    stacklevel=2,
                )
            return table_name, extracted["resources"], self.RESOURCE_SCHEMA

        return f"{table_name}_errors", extracted["errors"], self.ERROR_SCHEMA

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
        include_resources: bool = True,
    ) -> RCSBPDBResolution:
        """Resolve a PDB ID or RCSB DOI to normalized entry metadata."""
        kind = self.classify_identifier(identifier)

        if kind == "rcsbpdb_doi":
            doi = self.normalize_doi(identifier)
            pdb_id = self.extract_pdb_id_from_doi(doi)
            return self.lookup_rcsbpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=doi,
                query_source=query_source or "identifier",
                include_resources=include_resources,
            )

        if kind == "pdb_id":
            pdb_id = self.normalize_pdb_id(identifier)
            return self.lookup_rcsbpdb(
                pdb_id=pdb_id,
                original_identifier=identifier,
                doi=None,
                query_source=query_source or "identifier",
                include_resources=include_resources,
            )

        return RCSBPDBResolution(
            original_identifier=str(identifier),
            normalized_identifier=str(identifier),
            repo="other",
            endpoint_used=None,
            status="skipped",
            query_source=query_source,
            notes=["Identifier did not match an RCSBPDB DOI or 4-character PDB ID."],
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
            resources.append(
                FileResource(
                    label=label,
                    url=url,
                    extension=extension,
                    source=source,
                    format_hint=format_hint,
                )
            )

        return resources

    def lookup_rcsbpdb(
        self,
        pdb_id: str | None,
        original_identifier: str,
        doi: str | None = None,
        query_source: str | None = None,
        include_resources: bool = True,
    ) -> RCSBPDBResolution:
        """Retrieve one RCSB entry and optionally build its resource pointers."""
        result = RCSBPDBResolution(
            original_identifier=original_identifier,
            normalized_identifier=doi or pdb_id or str(original_identifier),
            repo="rcsbpdb",
            endpoint_used=self.ENDPOINTS["entry"],
            endpoint_variables={"pdb_id": pdb_id},
            doi=doi,
            query_source=query_source,
            resources_loaded=include_resources,
        )

        if not pdb_id:
            result.notes.append("Could not resolve a 4-character PDB ID.")
            return result

        meta_url = self.ENDPOINTS["entry"].format(pdb_id=pdb_id)
        result.metadata_url = meta_url
        result.landing_page_url = self.ENDPOINTS["entry_landing"].format(pdb_id=pdb_id)

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

            if include_resources:
                result.files = self._build_file_resources(pdb_id, meta)

            return result

        except requests.HTTPError as exc:
            result.status = f"http_error_{getattr(exc.response, 'status_code', 'unknown')}"
            result.notes.append("RCSB entry metadata request failed.")
            return result
        except requests.RequestException as exc:
            result.status = "request_failed"
            result.notes.append(f"RCSBPDB request error: {exc!s}")
            return result
        except Exception as exc:
            result.status = "parse_failed"
            result.notes.append(f"RCSBPDB parsing error: {exc!s}")
            return result


    # ------------------------------------------------------------------
    # Loading and normalization
    # ------------------------------------------------------------------



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
                    "resource_count": (
                        len(res.files) if res.resources_loaded else None
                    ),
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
            "datasets": datasets,
            "resources": resources,
            "errors": errors,
        }

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
        """Return a cached table as a DataFrame or ordered dictionary."""
        resolved = self._resolve_table_name(table_name)
        if resolved not in self._cache:
            raise ValueError(
                f"Table '{table_name}' not found in the cache. "
                f"Available tables: {list(self._cache.keys())}"
            )
        table = self._cache[resolved]
        if dict_return:
            return table
        return pd.DataFrame(table)


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
            quote = value_str[0]
            value_str = value_str[1:-1]
            if quote == "'":
                value_str = value_str.replace("''", "'")

        try:
            if "." not in value_str:
                return int(value_str)
            return float(value_str)
        except ValueError:
            return value_str

    def get_schema(self) -> str:
        """Describe supported RCSBPDB queries and their return types."""
        return """
RCSBPDB webserver query schema

Initialization
--------------
RCSBPDB() validates the RCSB connection and creates an empty OrderedDict cache.
Initialization does not search for PDB entries or retrieve resource tables.
Connection validation does make a request to the RCSB Data API.

Each dictionary query returns one pandas DataFrame and saves one ordered
column-oriented table in the cache. The result's attrs['table_name'] gives the
cached table name. Repeating a query with the same table name replaces that
table; tables with other names remain available. Use list(), get_table(),
display(), and summary() to explore cached tables.

Cached-data exploration
-----------------------
query_artifacts("text") searches cached rows and returns a list of DSI
ValueObjects. query_artifacts("column = value") filters cached rows using
find_relation() and also returns a list of ValueObjects. These string queries
do not search the remote RCSB API.

Tier 1: dataset discovery
-------------------------
Example: query_artifacts({"keywords": "hemoglobin", "limit": 5})
Search filters include keywords, authors, and experimental_method. limit and
start control pagination; return_type is passed to the RCSB Search API.
Returns one DataFrame of high-level entries, with columns such as dataset_id,
doi, title, experimental_method and release_date. Resource pointers are not
loaded at this tier, so resource_count is not populated. The table name is
pdb_datasets_<query token>. Select dataset_id to retrieve its resources.

Tier 2: selected PDB entry
--------------------------
Examples: {"pdb_id": "4HHB"}, {"doi": "10.2210/pdb4hhb/pdb"}, or
          {"identifiers": ["1CBS", "4HHB"]}.
Returns one DataFrame of resource metadata and download URLs, including
dataset_id, name, format, resource_type and download_url. The query does not
download files. Resource URLs are unverified candidates and may not exist;
call validate_urls(result.attrs['table_name']) to check them explicitly.
Cache names start with pdb_resources_ and include resolved PDB IDs.
If there are errors and no successful rows, an error table is returned instead.
For mixed successful and failed lookups, the successful table is returned and
a warning identifies failed entries. Query them individually for error details.
""".strip()


    def num_tables(self):
        table_count = len(self._cache)

        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")

    def validate_urls(self, table_name: str, url_column: str = "download_url", **kwargs):
        """Check URLs in a named cached table; queries do not check them."""
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
                        timeout=self.REQUEST_TIMEOUT,
                        verify=self.verify,
                    )
                    status_code = response.status_code
                    method_used = "HEAD"

                    if not (200 <= status_code < 400):
                        response.close()
                        response = self.session.get(
                            url,
                            stream=True,
                            allow_redirects=True,
                            timeout=self.REQUEST_TIMEOUT,
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
        """Retrieve one remote table or explore data already stored in cache.

        A dictionary performs a tiered RCSB API retrieval and returns exactly
        one DataFrame. A string performs a local cache search and returns a
        list of ValueObjects through find_cell() or find_relation().
        """
        if not (
            isinstance(query, dict)
            or (isinstance(query, str) and query.strip())
        ):
            raise ValueError(
                "Input query must be a dictionary of parameters or a nonempty string."
            )

        if isinstance(query, str):
            operators = ["==", "!=", ">=", "<=", "=", "<", ">", "~", "~~"]

            if not any(operator in query for operator in operators) and not re.search(
                r"\([^)]*\)",
                query,
            ):
                matches = self.find_cell(query, row=True)
            else:
                column_name, relation = self.check_relational_query(
                    query,
                    operators,
                )
                matches = self.find_relation(column_name, relation)

            if not matches:
                print(f"WARNING: No cached values matched the query: {query}")

            return matches

        params = dict(query)
        self._validate_params(params)
        identifiers = self._extract_identifiers_from_params(params)

        if identifiers:
            table_name, rows, schema = self._query_selected_entries(identifiers)
        else:
            table_name, rows, schema = self._query_discovery(params)

        # Store this one query result as a column-oriented ordered table.
        table = self._rows_to_table(rows or [], schema)
        self._cache[table_name] = table
        self._loaded = True

        # Return the user-facing result as one DataFrame.
        dataframe = pd.DataFrame(table)
        dataframe.attrs["table_name"] = table_name
        return dataframe


    def notebook(self, **kwargs):
        """
        Notebook generation is not supported for the rcsbpdb backend.
        """
        raise NotImplementedError("Notebook generation is not supported for the rcsbpdb backend.")

    def process_artifacts(self):
        """Return cached tables for DSI writers and processing."""
        return self._cache

    # DEPRECATING SOON
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

    # DEPRECATING SOON
    def find_table(self, query_object, **kwargs):
        if not isinstance(query_object, str):
            raise TypeError("find_table() ERROR: query_object must be a string")

        matches = []

        for table_name, table_data in self._cache.items():
            if query_object in table_name.lower():
                val = self.ValueObject()
                val.t_name = table_name
                val.c_name = list(table_data.keys())
                val.value = table_data
                val.type = "table"

                matches.append(val)

        return matches

    # DEPRECATING SOON
    def find_column(self, query_object, **kwargs):
        if not isinstance(query_object, str):
            raise TypeError("find_column() ERROR: query_object must be a string")

        matches = []

        for table_name, table_data in self._cache.items():
            for col_name, col_data in table_data.items():
                if query_object in col_name.lower():
                    val = self.ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    val.value = col_data
                    val.type = "column"

                    matches.append(val)

        return matches

    def find_cell(self, query_object, row=False, **kwargs):
        """
        Search cached tables for matching cell values.

        Parameters
        ----------
        query_object : Any
            Value to search for in the cached tables.

        row : bool, optional
            If False, return matching individual cells.
            If True, return the complete row containing each match.

        Returns
        -------
        list
            A list of DSI ValueObjects. An empty list is returned if there
            are no matches.

        Notes
        -----
        String matching is case-insensitive and supports partial matches.
        Row numbers start at 1, consistent with SQLite's ValueObjects.
        """
        matches = []

        is_string_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_string_query else None

        for table_name, table_data in self._cache.items():
            if not table_data:
                continue

            dataframe = pd.DataFrame(table_data)

            if dataframe.empty:
                continue

            columns = list(dataframe.columns)

            for row_index, row_data in dataframe.iterrows():
                row_matched = False

                for column in columns:
                    cell = row_data[column]
                    match = False

                    # Handle missing scalar values without applying pd.isna()
                    # as a Boolean to a list or dictionary.
                    if pd.api.types.is_scalar(cell):
                        cell_is_missing = bool(pd.isna(cell))
                    else:
                        cell_is_missing = False

                    if pd.api.types.is_scalar(query_object):
                        query_is_missing = bool(pd.isna(query_object))
                    else:
                        query_is_missing = False

                    if cell_is_missing or query_is_missing:
                        match = cell_is_missing and query_is_missing

                    elif is_string_query and isinstance(cell, str):
                        match = query_lower in cell.lower()

                    elif is_string_query and isinstance(cell, (dict, list, tuple)):
                        match = query_lower in str(cell).lower()

                    elif pd.api.types.is_scalar(cell) and pd.api.types.is_scalar(
                        query_object
                    ):
                        match = bool(cell == query_object)

                    if not match:
                        continue

                    if row:
                        row_matched = True
                        break

                    value_object = self.ValueObject()
                    value_object.t_name = table_name
                    value_object.c_name = [column]
                    value_object.row_num = int(row_index) + 1
                    value_object.value = cell
                    value_object.type = "cell"
                    matches.append(value_object)

                if row and row_matched:
                    value_object = self.ValueObject()
                    value_object.t_name = table_name
                    value_object.c_name = columns
                    value_object.row_num = int(row_index) + 1
                    value_object.value = row_data.tolist()
                    value_object.type = "row"
                    matches.append(value_object)

        return matches

    def find_relation(self, column_name, relation, **kwargs):
        """Return ValueObjects for relational filters over cached tables.

        Parameters
        ----------
        column_name : str
            Name of the column to filter.

        relation : str
            Comparison operator and value, for example ``"> 5"`` or
            ``"= 1CBS"``.

        Returns
        -------
        list
            A list of matching DSI ValueObjects.
        """
        if not isinstance(column_name, str) or not isinstance(relation, str):
            raise TypeError(
                "find_relation() expects a string column name and relation."
            )

        operator, value = self._parse_relation(relation)
        matches = []

        for table_name in self.list(collection=True):
            dataframe = self.get_table(table_name, dict_return=False)

            if dataframe.empty or column_name not in dataframe.columns:
                continue

            # Numeric ordering and range comparisons require numeric values.
            # Equality, inequality, and text matching must preserve text
            # columns such as PDB IDs.
            filter_data = dataframe
            if operator in {">", "<", ">=", "<=", "range"}:
                filter_data = dataframe.copy()
                filter_data[column_name] = pd.to_numeric(
                    filter_data[column_name],
                    errors="coerce",
                )
                filter_data = filter_data.dropna(subset=[column_name])

            filtered = self._apply_pandas_filter(
                filter_data,
                column_name,
                operator,
                value,
            )

            for index in filtered.index:
                row = dataframe.loc[index]
                value_object = self.ValueObject()
                value_object.t_name = table_name
                value_object.c_name = list(dataframe.columns)
                value_object.row_num = int(index) + 1
                value_object.value = row.tolist()
                value_object.type = "relation"
                matches.append(value_object)

        return matches

    
    def list(self, collection=False, **kwargs):
        table_names = list(self._cache.keys())

        if collection:
            return table_names

        for table_name in table_names:
            df = self.get_table(table_name)
            print(f"\nTable: {table_name}")
            print(f"  - num of columns: {df.shape[1]}")
            print(f"  - num of rows: {df.shape[0]}")
        print()

    def display(self, table_name, num_rows=25, display_cols=None, **kwargs):
        """
        Return a preview DataFrame from a table.

        Parameters
        ----------
        table_name : str
            Name of the table to display.
        num_rows : int, optional
            Number of rows to return.
        display_cols : list[str], optional
            Specific columns to display.
        """
        resolved = self._resolve_table_name(table_name)

        if resolved not in self._cache:
            raise ValueError(
                f"display() could not find table '{table_name}'. "
                f"Available tables: {self.list(True)}"
            )

        df = self.get_table(resolved)

        if df.empty:
            raise ValueError(f"Table '{resolved}' is empty.")

        max_rows = len(df)

        if display_cols is not None:
            missing_cols = [col for col in display_cols if col not in df.columns]

            if missing_cols:
                raise ValueError(
                    f"display() could not find column(s) {missing_cols} "
                    f"in table '{resolved}'. Available columns: {list(df.columns)}"
                )

            df = df[display_cols]

        if num_rows:
            df = df.head(num_rows)

        df.attrs["max_rows"] = max_rows
        return df.astype(object).where(df.notna(), None)


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
        """Release HTTP resources and clear the ordered in-memory cache."""
        if hasattr(self, "session"):
            self.session.close()

        self._cache = OrderedDict()
        self._loaded = False


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()