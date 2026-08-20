"""
Zenodo Webserver Backend for DSI.

Read-only backend that pulls public metadata from Zenodo and exposes it as
two stable in-memory DSI tables:

    datasets
    resources

Design
------
- datasets: one row per Zenodo record
- resources: one row per file attached to any Zenodo record
- no errors table

Supported inputs
----------------
- params={"keywords": "...", "limit": 5}
- params={"q": "...", "limit": 5}
- params={"record_id": "16537543"}
- params={"doi": "10.5281/zenodo.16537543"}

Convenience inputs are also supported:
- Zenodo(keywords="climate", limit=5)
- Zenodo(record_id="16537543")
- Zenodo(doi="10.5281/zenodo.16537543")

Official Zenodo API used
------------------------
GET https://zenodo.org/api/records
GET https://zenodo.org/api/records/{record_id}
"""

from __future__ import annotations

import json
import os
import re
import ssl
from collections import OrderedDict
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util.retry import Retry

try:
    import truststore
except ImportError:
    truststore = None

from dsi.backends.webserver import Webserver


class ValueObject:
    """
    Container for search results returned by find* methods.
    """

    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""

    def to_dict(self):
        return {
            "t_name": self.t_name,
            "c_name": self.c_name,
            "row_num": self.row_num,
            "value": self.value,
            "type": self.type,
        }


class TruststoreAdapter(HTTPAdapter):
    """
    Optional TLS adapter for environments that prefer OS trust stores.
    """

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        if truststore is None:
            return super().init_poolmanager(
                connections,
                maxsize,
                block=block,
                **pool_kwargs,
            )

        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
            **pool_kwargs,
        )


class Zenodo(Webserver):
    """
    Zenodo read-only Webserver backend for DSI.

    This backend exposes two stable DSI tables:
        - datasets
        - resources
    """

    read_only = True

    DEFAULT_URL = "https://zenodo.org"

    DOI_REGEX = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.I)
    ZENODO_DOI_REGEX = re.compile(r"^10\.5281/zenodo\.(\d+)$", re.I)
    RECORD_ID_REGEX = re.compile(r"^\d+$")

    SUPPORTED_PARAMS = {
        "keywords",
        "q",
        "doi",
        "DOI",
        "record_id",
        "recordId",
        "recordID",
        "limit",
        "size",
        "page",
        "sort",
        "communities",
        "resource_type",
        "access_right",
    }

    DATASET_COLUMNS = [
        "dataset_id",
        "concept_record_id",
        "doi",
        "concept_doi",
        "title",
        "description",
        "source_repository",
        "landing_page",
        "metadata_url",
        "publication_date",
        "resource_type",
        "access_right",
        "license",
        "creators",
        "keywords",
        "version",
        "communities",
        "resource_count",
        "usability_label",
        "api_status",
        "query_source",
        "raw_metadata",
        "notes",
    ]

    RESOURCE_COLUMNS = [
        "resource_id",
        "dataset_id",
        "source_repository",
        "dataset_title",
        "name",
        "download_url",
        "format",
        "size",
        "checksum",
        "mimetype",
        "resource_type",
        "source",
        "url_valid",
        "raw_metadata",
    ]

    def __init__(
        self,
        url=None,
        params=None,
        keywords=None,
        record_id=None,
        doi=None,
        limit=None,
        **kwargs,
    ) -> None:
        """
        Initialize Zenodo backend.
        """
        base_url = url or self.DEFAULT_URL

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid Zenodo base URL")

        self.base_url = base_url.rstrip("/")
        self.records_api = f"{self.base_url}/api/records"
        self.validate_error_msg = None

        # skip data retrieval if only checking connection to zenodo
        if kwargs.get("only_validate", False):
            return

        self.timeout = kwargs.get("timeout", 60)
        self.verify_ssl = kwargs.get("verify_ssl", kwargs.get("verify", True))
        self.validate_resource_urls = kwargs.get("validate_resource_urls", False)
        self.retries = kwargs.get("retries", 3)
        self.use_truststore = kwargs.get("use_truststore", False)
        self.token = kwargs.get("token", os.getenv("ZENODO_TOKEN"))
        self.validate_on_init = kwargs.get("validate_on_init", True)
        self.auto_load = kwargs.get("auto_load", True)

        self.headers = {
            "User-Agent": "dsi-zenodo-backend/1.0",
            "Accept": "application/json",
        }

        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        self.session = self._create_session(self.retries)

        self._cache = OrderedDict()
        self.tables = self._cache
        self._loaded = False

        self.params = self._build_params(
            params=params,
            keywords=keywords,
            record_id=record_id,
            doi=doi,
            limit=limit,
        )

        self.last_search_response = None
        self.last_request_params = None
        self.raw_records = []

        self._initialize_empty_tables()

        if self.validate_on_init and not self.validate_connection():
            self._loaded = False
            raise ConnectionError(self.validate_error_msg or "Validating Zenodo connection failed.")

        if self.params and self.auto_load:
            self._load_initial_data(self.params)

        self._loaded = True

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _build_params(
        self,
        params=None,
        keywords=None,
        record_id=None,
        doi=None,
        limit=None,
    ):
        if params is not None and any(
            value is not None for value in [keywords, record_id, doi]
        ):
            raise ValueError(
                "Use either params or convenience inputs "
                "(keywords, record_id, doi), not both."
            )

        if params is not None:
            built = dict(params)
        else:
            built = {}

            if keywords is not None:
                built["keywords"] = keywords

            if record_id is not None:
                built["record_id"] = record_id

            if doi is not None:
                built["doi"] = doi

        if limit is not None:
            built["limit"] = limit

        return built

    def _initialize_empty_tables(self):
        self._cache["datasets"] = OrderedDict(
            {col: [] for col in self.DATASET_COLUMNS}
        )
        self._cache["resources"] = OrderedDict(
            {col: [] for col in self.RESOURCE_COLUMNS}
        )
        self.tables = self._cache

    def _create_session(self, retries):
        session = requests.Session()
        session.headers.update(self.headers)

        retry_strategy = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
        )

        if self.use_truststore:
            adapter = TruststoreAdapter(max_retries=retry_strategy)
        else:
            adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    # ------------------------------------------------------------------
    # Required Webserver methods
    # ------------------------------------------------------------------

    def validate_connection(self):
        """
        Validate that Zenodo Records API is reachable.

        Return
        ------
        bool
            True if connection is valid, False otherwise.
        """
        try:
            response = self.session.get(
                self.records_api,
                params={"size": 1},
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()

            data = response.json()
            if "hits" not in data:
                self.validate_error_msg = "Zenodo Records API returned unexpected response."
                return False

            return True

        except Exception as e:
            self.validate_error_msg = f"Unable to connect to Zenodo API: {e}"
            return False

    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Zenodo is read-only.
        """
        raise NotImplementedError("Zenodo backend is read-only.")

    def query_artifacts(self, query=None, dict_return=True, **kwargs):
        """
        Query Zenodo or filter already-loaded DSI tables.

        Behavior
        --------
        query is dict:
            Re-query Zenodo API and rebuild datasets/resources.

        query is str and looks like a filter:
            Apply pandas query syntax to loaded tables.

        query is str and does not look like a filter:
            Treat it as a Zenodo keyword search.

        query is None:
            Return loaded tables.
        """
        if query is None:
            return self._cache if dict_return else self.get_tables_as_dataframes()

        if isinstance(query, dict):
            self.params = query
            self._load_initial_data(self.params)
            return self._cache if dict_return else self.get_tables_as_dataframes()

        if isinstance(query, str):
            looks_like_filter = any(
                op in query
                for op in ["==", "!=", ">=", "<=", ">", "<", " in ", ".str"]
            )

            if looks_like_filter:
                return self._query_loaded_tables(query, dict_return=dict_return)

            query_params = {"keywords": query}
            query_params.update(kwargs)
            self.params = query_params
            self._load_initial_data(self.params)
            return self._cache if dict_return else self.get_tables_as_dataframes()

        raise TypeError("query_artifacts expects None, str, or dict.")

    def get_table(self, table_name, dict_return=False, **kwargs):
        if not self._loaded:
            raise RuntimeError("No data loaded.")

        resolved_name = self._resolve_table_name(table_name)
        table = self._cache.get(resolved_name)

        if table is None:
            raise ValueError(f"Table '{resolved_name}' not found.")

        if dict_return:
            return table

        return pd.DataFrame(table)

    def notebook(self, **kwargs):
        """
        Notebook generation is not supported for the Zenodo backend.
        """
        raise NotImplementedError(
            "Notebook generation is not supported for the Zenodo backend."
        )

    def process_artifacts(self, **kwargs):
        if not self._loaded:
            return OrderedDict()

        return self._cache

    def get_schema(self):
        schema_lines = []

        for table_name, table in self._cache.items():
            cols = []

            for col_name, values in table.items():
                dtype = "TEXT"

                for value in values:
                    if value is None:
                        continue

                    if isinstance(value, bool):
                        dtype = "BOOLEAN"
                    elif isinstance(value, int):
                        dtype = "INTEGER"
                    elif isinstance(value, float):
                        dtype = "REAL"

                    break

                cols.append(f"    {col_name} {dtype}")

            create_stmt = (
                f"CREATE TABLE {table_name} (\n"
                + ",\n".join(cols)
                + "\n);"
            )
            schema_lines.append(create_stmt)

        return "\n\n".join(schema_lines)

    # ------------------------------------------------------------------
    # Find/search methods
    # ------------------------------------------------------------------

    def find(self, query_object, **kwargs):
        """
        Search table names, column names, and cell values.

        Returns
        -------
        list[ValueObject]
            Combined results from find_table(), find_column(), and find_cell().
        """
        return (
            self.find_table(query_object, **kwargs)
            + self.find_column(query_object, **kwargs)
            + self.find_cell(query_object, **kwargs)
        )

    def find_table(self, query_object, **kwargs):
        """
        Search input across table names.

        Returns
        -------
        list[ValueObject]
        """
        if query_object is None:
            return []

        query = str(query_object).lower()
        matches = []

        for table_name, table_data in self._cache.items():
            if query in table_name.lower():
                val = ValueObject()
                val.t_name = table_name
                val.c_name = list(table_data.keys())
                val.row_num = None
                val.value = table_data
                val.type = "table"
                matches.append(val)

        return matches

    def find_column(self, query_object, range=False, **kwargs):
        """
        Search input across column names.

        Returns
        -------
        list[ValueObject]
        """
        if query_object is None:
            return []

        query = str(query_object).lower()
        matches = []

        for table_name, table_data in self._cache.items():
            for col_name, col_data in table_data.items():
                if query in col_name.lower():
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    val.row_num = None
                    val.value = col_data
                    val.type = "column"

                    if range:
                        numeric_col = pd.to_numeric(
                            pd.Series(col_data),
                            errors="coerce",
                        ).dropna()

                        if not numeric_col.empty:
                            val.value = {
                                "min": numeric_col.min(),
                                "max": numeric_col.max(),
                            }

                    matches.append(val)

        return matches

    def find_cell(self, query_object, row=True, **kwargs):
        """
        Search input across cell values.

        The returned ValueObjects represent full matching rows, not only the
        matched cell. This matches the RCSBPDB-style DSI find behavior.
        """
        if query_object is None:
            return []

        matches = []
        seen_rows = set()

        is_str_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str_query else None

        for table_name, table_data in self._cache.items():
            df = pd.DataFrame(table_data)

            if df.empty:
                continue

            for row_idx, row_data in df.iterrows():
                row_matched = False

                for cell in row_data.tolist():
                    if query_object == cell:
                        row_matched = True
                        break

                    if is_str_query and query_lower in str(cell).lower():
                        row_matched = True
                        break

                if row_matched:
                    key = (table_name, int(row_idx))
                    if key in seen_rows:
                        continue

                    seen_rows.add(key)
                    matches.append(
                        self._row_to_value_object(
                            table_name=table_name,
                            row_num=int(row_idx),
                            row=row_data.to_dict(),
                            value_type="row",
                        )
                    )

        return matches

    def find_relation(self, column_name, relation=None, **kwargs):
        """
        Filter rows using a column-level relation.

        Supported call styles
        ---------------------
        1. Column + relation:
            find_relation("resource_count", ">= '1'")
            find_relation("format", "= 'csv'")
            find_relation("title", "~ 'climate'")

        2. One-string condition:
            find_relation("resource_count >= 1")
            find_relation("format = csv")
            find_relation("title ~ climate")

        3. Zenodo API-backed lookup/search:
            find_relation("record_id = 16537543")
            find_relation("dataset_id = 16537543")
            find_relation("doi = 10.5281/zenodo.16537543")
            find_relation("keywords ~ climate")
            find_relation("q ~ climate")

        Returns
        -------
        list[ValueObject]
            For local table filtering.

        OrderedDict
            For Zenodo API-backed lookup/search calls. The returned object is
            self.tables after reloading datasets/resources from the API.
        """
        if relation is None:
            column_name, relation = self._parse_condition_string(column_name)

        if column_name is None or relation is None:
            raise ValueError("find_relation requires a condition or column + relation.")

        column_name = str(column_name).strip()
        relation = str(relation).strip()

        parsed = self._parse_relation(relation)
        if parsed is None:
            raise ValueError(
                "Could not parse relation. Expected examples: "
                ">= '1', = 'csv', ~ 'climate'."
            )

        op, target_value = parsed

        api_result = self._maybe_api_backed_find_relation(
            column_name=column_name,
            op=op,
            target_value=target_value,
            **kwargs,
        )
        if api_result is not None:
            return api_result

        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot find relation.")

        matches = []

        for table_name, table in self._cache.items():
            df = pd.DataFrame(table)

            if df.empty or column_name not in df.columns:
                continue

            mask = self._evaluate_relation(df[column_name], op, target_value)

            for row_num in df[mask].index:
                row_dict = df.loc[row_num].to_dict()
                matches.append(
                    self._row_to_value_object(
                        table_name=table_name,
                        row_num=int(row_num),
                        row=row_dict,
                        value_type="row",
                    )
                )

        return matches

    def list(self, collection=False, **kwargs):
        if collection:
            return list(self._cache.keys())

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"{name}: ({len(df)} rows, {len(df.columns)} cols)")

    def num_tables(self):
        if not self._loaded:
            print("0 tables loaded")
            return 0

        table_count = len(self._cache)
        print(f"{table_count} tables loaded")
        return table_count

    def display(self, table_name, num_rows=25, display_cols=None, **kwargs):
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        resolved_name = self._resolve_table_name(table_name)
        table = self._cache.get(resolved_name)

        if table is None:
            raise ValueError(f"Table '{resolved_name}' not found.")

        df = pd.DataFrame(table)

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in '{resolved_name}': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        max_rows = len(df)

        if num_rows:
            df = df.head(num_rows)

        df = self._map_dataframe(df, lambda x: self._truncate_cell(x, max_chars=80))
        df.attrs["max_rows"] = max_rows

        return df

    def summary(self, table_name=None, **kwargs):
        if not self._loaded:
            return pd.DataFrame()

        if table_name:
            resolved_name = self._resolve_table_name(table_name)
            table = self._cache.get(resolved_name)

            if table is None:
                raise ValueError(f"Table '{resolved_name}' not found.")

            df = pd.DataFrame(table)

            return pd.DataFrame(
                [
                    {
                        "table_name": resolved_name,
                        "num_rows": len(df),
                        "num_columns": len(df.columns),
                        "columns": list(df.columns),
                    }
                ]
            )

        table_names = []
        summary_dfs = []

        for name, table in self._cache.items():
            df = pd.DataFrame(table)

            summary_dict = {
                "table_name": name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
            }

            table_names.append(name)
            summary_dfs.append(pd.DataFrame([summary_dict]))

        return [table_names] + summary_dfs

    def close(self):
        if hasattr(self, "session"):
            self.session.close()

        self._cache = OrderedDict()
        self.tables = self._cache
        self.raw_records = []
        self.last_search_response = None
        self.last_request_params = None
        self.params = {}
        self._loaded = False

    # ------------------------------------------------------------------
    # Zenodo request helpers
    # ------------------------------------------------------------------

    def _request(self, url, params=None):
        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        response.raise_for_status()

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(f"Zenodo response was not valid JSON: {url}") from exc

    def _load_initial_data(self, params):
        self._validate_params(params)

        self._initialize_empty_tables()

        records = self._records_from_params(params)
        self.raw_records = records

        dataset_rows, resource_rows = self._extract_tables(records)

        self._cache["datasets"] = self._rows_to_table_with_columns(
            dataset_rows,
            self.DATASET_COLUMNS,
        )
        self._cache["resources"] = self._rows_to_table_with_columns(
            resource_rows,
            self.RESOURCE_COLUMNS,
        )

        self.tables = self._cache
        self._loaded = True

    def _records_from_params(self, params):
        records = []

        record_ids = self._get_param_values(
            params,
            ["record_id", "recordId", "recordID"],
        )

        if record_ids:
            for record_id in record_ids:
                normalized = self.normalize_record_id(record_id)
                if not normalized:
                    raise ValueError(
                        f"Invalid Zenodo record_id: {record_id}. "
                        "Zenodo record IDs must be numeric."
                    )

                rec = self._lookup_record(normalized)
                if rec:
                    records.append(rec)

            return records

        dois = self._get_param_values(params, ["doi", "DOI"])

        if dois:
            for doi_value in dois:
                doi = self.normalize_doi(doi_value)
                if not doi:
                    raise ValueError(
                        f"Invalid DOI: {doi_value}. "
                        "Expected a DOI such as '10.5281/zenodo.16537543'."
                    )

                record_id = self.extract_record_id_from_doi(doi)

                if record_id:
                    rec = self._lookup_record(record_id)

                    if rec:
                        md = rec.get("metadata", {}) or {}
                        returned_doi = self.normalize_doi(md.get("doi"))

                        if self.dois_equal(doi, returned_doi):
                            records.append(rec)

                    continue

                matches = self._search_by_exact_doi(doi)
                records.extend(matches)

            return records

        return self._search_records(params)

    @staticmethod
    def _get_param_values(params, names):
        for name in names:
            value = params.get(name)
            if value is not None:
                if isinstance(value, list):
                    return value
                return [value]

        return []

    def _lookup_record(self, record_id):
        url = f"{self.records_api}/{record_id}"

        self.last_request_params = {
            "mode": "record_lookup",
            "record_id": record_id,
            "url": url,
        }

        try:
            return self._request(url)
        except requests.HTTPError as exc:
            response = exc.response

            if response is not None and response.status_code == 404:
                return None

            raise
        except requests.RequestException:
            return None

    def _search_by_exact_doi(self, doi):
        request_params = {
            "q": f'doi:"{doi}"',
            "size": 5,
            "page": 1,
        }

        self.last_request_params = request_params

        try:
            data = self._request(self.records_api, params=request_params)
            self.last_search_response = data
        except requests.RequestException:
            return []

        hits = data.get("hits", {}).get("hits", [])
        matches = []

        for rec in hits:
            md = rec.get("metadata", {}) or {}
            returned_doi = self.normalize_doi(md.get("doi"))

            if self.dois_equal(doi, returned_doi):
                matches.append(rec)

        return matches

    def _search_records(self, params):
        self._validate_params(params)

        try:
            limit = int(params.get("limit", params.get("size", 25)))
            page = int(params.get("page", 1))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Zenodo 'limit', 'size', and 'page' must be integers."
            ) from exc

        if limit < 1:
            raise ValueError("Zenodo 'limit' or 'size' must be greater than 0.")

        if page < 1:
            raise ValueError("Zenodo 'page' must be greater than 0.")

        q = params.get("q") or self._build_search_query(params)

        if not q:
            return []

        request_params = {
            "q": q,
            "size": limit,
            "page": page,
        }

        if params.get("sort"):
            request_params["sort"] = params["sort"]

        self.last_request_params = request_params

        data = self._request(self.records_api, params=request_params)
        self.last_search_response = data

        return data.get("hits", {}).get("hits", [])

    def _validate_params(self, params):
        unsupported = set(params.keys()) - self.SUPPORTED_PARAMS

        if unsupported:
            raise ValueError(
                f"Unsupported Zenodo search params: {sorted(unsupported)}. "
                f"Supported params: {sorted(self.SUPPORTED_PARAMS)}"
            )

    def _build_search_query(self, params):
        clauses = []

        keywords = params.get("keywords")
        if keywords:
            clauses.append(str(keywords))

        communities = params.get("communities")
        if communities:
            clauses.append(f"communities:{communities}")

        resource_type = params.get("resource_type")
        if resource_type:
            clauses.append(f"resource_type.type:{resource_type}")

        access_right = params.get("access_right")
        if access_right:
            clauses.append(f"access_right:{access_right}")

        if not clauses:
            return None

        return " AND ".join(clauses)

    # ------------------------------------------------------------------
    # Table extraction
    # ------------------------------------------------------------------

    def _extract_tables(self, records):
        dataset_rows = []
        resource_rows = []

        query_source = self._query_source_label()

        for rec in records:
            md = rec.get("metadata", {}) or {}
            links = rec.get("links", {}) or {}

            record_id = (
                str(rec.get("id"))
                if rec.get("id") is not None
                else None
            )
            concept_record_id = (
                str(rec.get("conceptrecid"))
                if rec.get("conceptrecid") is not None
                else None
            )

            doi = self.normalize_doi(md.get("doi"))
            concept_doi = self.normalize_doi(md.get("conceptdoi"))

            title = md.get("title") or f"zenodo_{record_id}"

            landing_page = (
                links.get("self_html")
                or links.get("html")
                or (f"{self.base_url}/records/{record_id}" if record_id else None)
            )

            metadata_url = (
                links.get("self")
                or (f"{self.records_api}/{record_id}" if record_id else None)
            )

            file_rows, file_exts = self._extract_resource_rows(
                rec=rec,
                record_id=record_id,
                title=title,
                md=md,
            )

            resource_rows.extend(file_rows)

            dataset_rows.append(
                {
                    "dataset_id": record_id,
                    "concept_record_id": concept_record_id,
                    "doi": doi,
                    "concept_doi": concept_doi,
                    "title": title,
                    "description": md.get("description"),
                    "source_repository": "Zenodo",
                    "landing_page": landing_page,
                    "metadata_url": metadata_url,
                    "publication_date": md.get("publication_date"),
                    "resource_type": md.get("resource_type"),
                    "access_right": md.get("access_right"),
                    "license": md.get("license"),
                    "creators": md.get("creators"),
                    "keywords": md.get("keywords"),
                    "version": md.get("version"),
                    "communities": md.get("communities"),
                    "resource_count": len(file_rows),
                    "usability_label": self.classify_usability(file_exts),
                    "api_status": "ok" if file_rows else "ok_no_files",
                    "query_source": query_source,
                    "raw_metadata": self._json_or_none(
                        {
                            "id": rec.get("id"),
                            "conceptrecid": rec.get("conceptrecid"),
                            "doi": md.get("doi"),
                            "conceptdoi": md.get("conceptdoi"),
                            "title": md.get("title"),
                            "description": md.get("description"),
                            "publication_date": md.get("publication_date"),
                            "resource_type": md.get("resource_type"),
                            "access_right": md.get("access_right"),
                            "creators": md.get("creators"),
                            "keywords": md.get("keywords"),
                            "license": md.get("license"),
                            "version": md.get("version"),
                            "communities": md.get("communities"),
                            "links": links,
                        }
                    ),
                    "notes": None,
                }
            )

        return dataset_rows, resource_rows

    def _extract_resource_rows(self, rec, record_id, title, md):
        files = rec.get("files", []) or []
        rows = []
        file_exts = []

        for idx, file_obj in enumerate(files, start=1):
            file_links = file_obj.get("links", {}) or {}
            file_url = file_links.get("self")

            label = file_obj.get("key")
            extension = self.get_file_ext(label or file_url)

            if extension:
                file_exts.append(extension)

            url_valid = None

            if self.validate_resource_urls and file_url:
                url_valid, _status_code, _content_type = self._url_exists(file_url)

                if url_valid is False:
                    continue

            rows.append(
                {
                    "resource_id": f"{record_id}:{idx}",
                    "dataset_id": record_id,
                    "source_repository": "Zenodo",
                    "dataset_title": title,
                    "name": label,
                    "download_url": file_url,
                    "format": extension,
                    "size": file_obj.get("size"),
                    "checksum": file_obj.get("checksum"),
                    "mimetype": file_obj.get("mimetype"),
                    "resource_type": md.get("resource_type"),
                    "source": "zenodo.files[]",
                    "url_valid": url_valid,
                    "raw_metadata": self._json_or_none(file_obj),
                }
            )

        return rows, file_exts

    def _url_exists(self, url):
        try:
            response = self.session.head(
                url,
                allow_redirects=True,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )

            if response.status_code == 405:
                response = self.session.get(
                    url,
                    stream=True,
                    allow_redirects=True,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )

            exists = 200 <= response.status_code < 400
            status_code = response.status_code
            content_type = response.headers.get("content-type")
            response.close()

            return exists, status_code, content_type

        except Exception:
            return None, None, None

    # ------------------------------------------------------------------
    # Find helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_value_object(table_name, row_num, row, value_type="row"):
        val = ValueObject()
        val.t_name = table_name
        val.c_name = list(row.keys())
        val.row_num = row_num
        val.value = row
        val.type = value_type
        return val

    def _maybe_api_backed_find_relation(
        self,
        column_name,
        op,
        target_value,
        **kwargs,
    ):
        """
        Convert specific find_relation inputs into Zenodo API lookups/searches.

        API-backed calls reload self.tables and return self.tables.
        """
        column = str(column_name).strip().lower()
        value = str(target_value).strip()
        limit = kwargs.get("limit")

        params = None

        if op in {"=", "=="} and column in {
            "record_id",
            "recordid",
            "dataset_id",
            "id",
        }:
            normalized = self.normalize_record_id(value)
            if not normalized:
                raise ValueError(
                    f"Invalid Zenodo record_id: {target_value}. "
                    "Zenodo record IDs must be numeric."
                )
            params = {"record_id": normalized}

        elif op in {"=", "=="} and column in {"doi", "concept_doi"}:
            doi = self.normalize_doi(value)
            if not doi:
                raise ValueError(
                    f"Invalid DOI: {target_value}. "
                    "Expected a DOI such as '10.5281/zenodo.16537543'."
                )
            params = {"doi": doi}

        elif op in {"~", "~~", "=", "=="} and column in {"keywords", "keyword"}:
            params = {"keywords": value}

        elif op in {"~", "~~", "=", "=="} and column == "q":
            params = {"q": value}

        elif op in {"~", "~~", "=", "=="} and column in {
            "communities",
            "resource_type",
            "access_right",
        }:
            params = {column: value}

        if params is None:
            return None

        if limit is not None:
            params["limit"] = limit
        elif "limit" in self.params:
            params["limit"] = self.params["limit"]

        self.params = params
        self._load_initial_data(params)
        return self.tables

    @staticmethod
    def _parse_condition_string(condition):
        if condition is None:
            raise ValueError("find_relation condition cannot be None.")

        condition = str(condition).strip()

        operators = ["~~", ">=", "<=", "!=", "==", ">", "<", "=", "~"]

        in_single = False
        in_double = False

        for idx, char in enumerate(condition):
            if char == "'" and not in_double:
                in_single = not in_single
            elif char == '"' and not in_single:
                in_double = not in_double

            if in_single or in_double:
                continue

            for op in operators:
                if condition.startswith(op, idx):
                    column_name = condition[:idx].strip()
                    value = condition[idx + len(op):].strip()

                    if not column_name or not value:
                        raise ValueError(
                            "One-string find_relation input must include "
                            "column, operator, and value."
                        )

                    return column_name, f"{op} {value}"

        raise ValueError(
            "Could not parse one-string find_relation condition. "
            "Expected examples: 'resource_count >= 1', 'format = csv'."
        )

    @staticmethod
    def _parse_relation(relation):
        operators = ["~~", ">=", "<=", "!=", "==", ">", "<", "=", "~"]

        relation = str(relation).strip()

        for op in operators:
            if relation.startswith(op):
                value = relation[len(op):].strip()
                value = value.strip("'").strip('"')
                return op, value

        return None

    @staticmethod
    def _evaluate_relation(series, op, target_value):
        if op in {"~", "~~"}:
            return series.fillna("").astype(str).str.contains(
                str(target_value),
                case=False,
                na=False,
                regex=False,
            )

        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_target = pd.to_numeric(
            pd.Series([target_value]),
            errors="coerce",
        ).iloc[0]

        use_numeric = pd.notna(numeric_target) and numeric_series.notna().any()

        if use_numeric and op in {">", "<", ">=", "<=", "=", "==", "!="}:
            comparisons = {
                ">": numeric_series > numeric_target,
                "<": numeric_series < numeric_target,
                ">=": numeric_series >= numeric_target,
                "<=": numeric_series <= numeric_target,
                "=": numeric_series == numeric_target,
                "==": numeric_series == numeric_target,
                "!=": numeric_series != numeric_target,
            }
            return comparisons[op].fillna(False)

        string_series = series.fillna("").astype(str)
        target = str(target_value)

        if op in {"=", "=="}:
            return string_series.str.lower() == target.lower()

        if op == "!=":
            return string_series.str.lower() != target.lower()

        return pd.Series([False] * len(series), index=series.index)

    # ------------------------------------------------------------------
    # Normalization / utility helpers
    # ------------------------------------------------------------------

    @classmethod
    def normalize_doi(cls, value):
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        for prefix in ("https://doi.org/", "http://doi.org/", "doi:", "DOI:"):
            s = s.replace(prefix, "")

        match = cls.DOI_REGEX.search(s)
        return match.group(1).lower().rstrip(" .;,)") if match else None

    @classmethod
    def normalize_record_id(cls, value):
        if value is None:
            return None

        s = str(value).strip()
        if cls.RECORD_ID_REGEX.match(s):
            return s

        return None

    @classmethod
    def extract_record_id_from_doi(cls, doi):
        if not doi:
            return None

        match = cls.ZENODO_DOI_REGEX.match(doi)
        return match.group(1) if match else None

    @staticmethod
    def get_file_ext(name_or_url):
        if not isinstance(name_or_url, str) or "." not in name_or_url:
            return None

        tail = name_or_url.split("?")[0].split("/")[-1].lower()

        compound_exts = [
            ".tar.gz",
            ".csv.gz",
            ".json.gz",
            ".txt.gz",
            ".xml.gz",
            ".pdf.gz",
        ]

        for ext in compound_exts:
            if tail.endswith(ext):
                return ext.lstrip(".")

        parts = tail.split(".")
        return parts[-1] if len(parts) > 1 else None

    @staticmethod
    def _truncate_cell(value, max_chars=80):
        if isinstance(value, str) and len(value) > max_chars:
            return value[:max_chars] + "..."
        return value

    @staticmethod
    def _json_or_none(value):
        if value is None:
            return None

        try:
            return json.dumps(value, sort_keys=True)
        except TypeError:
            return str(value)

    @staticmethod
    def _map_dataframe(df, func):
        if hasattr(df, "map"):
            return df.map(func)

        return df.applymap(func)

    @staticmethod
    def classify_usability(exts):
        ext_set = {e.lower() for e in exts if e}

        if not ext_set:
            return "unknown_format"

        tabular = {
            "csv",
            "tsv",
            "xlsx",
            "xls",
            "json",
            "xml",
            "txt",
            "parquet",
            "csv.gz",
            "json.gz",
            "txt.gz",
            "xml.gz",
        }

        scientific = {
            "nc",
            "h5",
            "hdf",
            "hdf5",
            "cdf",
            "fits",
            "mat",
            "npy",
            "npz",
            "zarr",
            "dat",
        }

        archive_only = {
            "zip",
            "tar",
            "tar.gz",
            "gz",
            "7z",
            "rar",
        }

        documents = {
            "pdf",
            "pdf.gz",
            "doc",
            "docx",
            "ppt",
            "pptx",
        }

        if ext_set & tabular:
            return "tabular_or_easy_parse"

        if ext_set & scientific:
            return "scientific_structured"

        if ext_set <= archive_only:
            return "archive_only"

        if ext_set <= documents:
            return "document_only"

        return "other_format"

    def dois_equal(self, a, b):
        return self.normalize_doi(a) == self.normalize_doi(b)

    def _query_source_label(self):
        if not self.params:
            return None

        if any(key in self.params for key in ["record_id", "recordId", "recordID"]):
            return "record_id"

        if any(key in self.params for key in ["doi", "DOI"]):
            return "doi"

        if "q" in self.params:
            return "q"

        if "keywords" in self.params:
            return "keywords"

        return "params"

    def _resolve_table_name(self, table_name):
        if table_name in self._cache:
            return table_name

        available = ", ".join(self._cache.keys())
        raise ValueError(
            f"Table '{table_name}' not found. Available tables: {available}"
        )

    def _rows_to_table_with_columns(self, rows, columns):
        table = OrderedDict({col: [] for col in columns})

        for row in rows:
            for col in columns:
                table[col].append(row.get(col))

        return table

    def get_tables(self):
        return self._cache

    def get_tables_as_dataframes(self):
        return {
            table_name: pd.DataFrame(table)
            for table_name, table in self._cache.items()
        }

    def _query_loaded_tables(self, query, dict_return=True):
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot query empty backend.")

        results = {}

        for table_name, table in self._cache.items():
            df = pd.DataFrame(table)

            if df.empty:
                continue

            try:
                result_df = df.query(query, engine="python")

                if not result_df.empty:
                    results[table_name] = (
                        result_df.to_dict(orient="list")
                        if dict_return
                        else result_df
                    )
            except pd.errors.UndefinedVariableError:
                continue
            except Exception as exc:
                raise ValueError(f"Query error in {table_name}: {exc}") from exc

        if not results:
            raise ValueError(f"Query returned no results: '{query}'")

        if not dict_return:
            if len(results) == 1:
                return next(iter(results.values()))

            return results

        return results

    def validate_urls(self):
        resources = self._cache.get("resources", {})
        urls = resources.get("download_url", [])

        valid_list = []

        for url in urls:
            if not url:
                valid_list.append(False)
                continue

            try:
                response = self.session.head(
                    url,
                    allow_redirects=True,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )

                if response.status_code == 405:
                    response = self.session.get(
                        url,
                        stream=True,
                        timeout=self.timeout,
                        verify=self.verify_ssl,
                    )

                valid_list.append(200 <= response.status_code < 400)
                response.close()

            except Exception:
                valid_list.append(False)

        resources["url_valid"] = valid_list
        return valid_list

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()