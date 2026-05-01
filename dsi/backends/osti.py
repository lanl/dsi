#!/usr/bin/env python3
"""
OSTI Backend for DSI

Read-only access that pulls metadata from REST-based OSTI backend
and exposes it as an in-memory DSI table: records
"""

import requests
import pandas as pd
from urllib.parse import urlparse
from collections import OrderedDict

from dsi.backends.webserver import Webserver

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------------------------
# Value Object (used for search results)
# ----------------------------------------------------------------------
class ValueObject:
    """
    Container for search results returned by find* methods

    Attributes
    ----------
    t_name : str
        Table name
    c_name : list
        Column name(s)
    row_num : int or None
        Row index (if applicable)
    value : any
        Matched value
    type : str
        {'table', 'column', 'cell'}
    """
    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""

# ---------------------------------------------------------
# OSTI Backend (Webserver - read only)
# ---------------------------------------------------------
class OSTI(Webserver):
    """
    REST-based web backend for querying OSTI metadata in-memory
    """

    # ----------------------------
    # Initialization
    # ----------------------------    
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from REST API.

        Parameters
        ----------

        `url` : str, optional
            Base OSTI URL. If None, a default OSTI endpoint is used.
        `params` : dict, optional
            Dictionary of initial query parameters used to fetch data from OSTI.
            Supported keys:
                "q",
                "osti_id",
                "doi",
                "fulltext",
                "biblio",
                "author",
                "title",
                "identifier",
                "sponsor_org",
                "research_org",
                "contributing_org",
                "source_id",
                "publication_date_start",
                "publication_date_end",
                "entry_date_start",
                "entry_date_end",
                "language",
                "country",
                "site_ownership_code",
                "subject",
                "has_fulltext",
                "sort",
                "order",
                "rows",
                "page",
        `**kwargs` : dict
            Additional keyword arguments:
                - api_key : str, optional
                    API key for authentication
                - verify_ssl : bool, optional
                    Toggle SSL verification (default False)
        """        

        DEFAULT_URL = "https://www.osti.gov/api/v1"
        base_url = url or DEFAULT_URL

        # ----------------------------------------------------------------------
        # Auth / connection config
        # ----------------------------------------------------------------------
        self.api_key = kwargs.get("api_key")
        self.verify_ssl = kwargs.get("verify_ssl", False)

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")

        # OSTI public API does not require authentication,
        # but we allow optional API key for future or private endpoints
        self.headers = {}
        if self.api_key:
            self.headers["Authorization"] = self.api_key

        # In-memory storage (DSI format)
        self._cache = OrderedDict()

        self._loaded = False
        self.params = params or {}

        # Validate connection FIRST before attempting to load data
        if not self.validate_connection():
            self._loaded = False
            raise ConnectionError(f"Unable to connect to OSTI API at {self.base_url}")

        # Initial data load
        if self.params:
            try:
                self._load_initial_data(self.params)
                self._loaded = True  # Data successfully loaded
            except Exception as e:
                self._loaded = False
                raise RuntimeError(f"Failed to load initial data: {e}") from e
        else:
            self._loaded = True  # Backend ready, no initial data to load

    # ----------------------------------------------------------------------
    # Connection Validation
    # ----------------------------------------------------------------------
    def validate_connection(self):
        """
        Validates that the base OSTI URL is accessible and functional.
        
        This method tests the connection by making a simple API call to verify:
            - The URL is reachable
            - The API responds with valid JSON
            - The response format is as expected (list of records)
        
        Returns
        -------
        bool
            True if connection is valid, False otherwise
        """
        try:
            # test_url = f"{self.base_url}/records"
            test_url = self.base_url + "/records"

            response = requests.get(
                test_url,
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=2,
                params={"rows": 1}  # minimal request

            )
            response.raise_for_status()

            data = response.json()

            # OSTI returns a list of records for /records
            if not isinstance(data, list):
                return False

            return True

        except Exception:  # noqa: E722
            # Silent failure to allow external workflows to continue
            return False

    # ---------------------------------------------------
    # Initial Data Load
    # ---------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch records from OSTI API and store in memory.

        params can be:
            dict       -> one OSTI request
            list[dict] -> multiple OSTI requests merged into one records table
        """

        if isinstance(params, dict):
            query_list = [params]

        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params

        else:
            raise TypeError("params must be a dict or a list of dicts")

        all_records = []

        for query_params in query_list:
            records = self._run_single_query(query_params)
            all_records.extend(records)

        unique_records = self._deduplicate_records(all_records)

        record_rows = self._extract_tables(unique_records)
        self._cache["records"] = self._rows_to_table(record_rows)

        self._loaded = True

    def _run_single_query(self, params):
        """
        Run one OSTI query and normalize the response to a list of records.
        """

        if "osti_id" in params and len(params) == 1:
            result = self._request(f"records/{params['osti_id']}")

            if isinstance(result, dict):
                return [result]

            if isinstance(result, list):
                return result

            return []

        request_params = self._build_request_params(params)

        result = self._request("records", request_params)

        if isinstance(result, list):
            return result

        if isinstance(result, dict):
            return [result]

        return []

    def _build_request_params(self, params):
        """
        Build OSTI /records query parameters from supported inputs.
        """

        supported_params = [
            "q",
            "osti_id",
            "doi",
            "fulltext",
            "biblio",
            "author",
            "title",
            "identifier",
            "sponsor_org",
            "research_org",
            "contributing_org",
            "source_id",
            "publication_date_start",
            "publication_date_end",
            "entry_date_start",
            "entry_date_end",
            "language",
            "country",
            "site_ownership_code",
            "subject",
            "has_fulltext",
            "sort",
            "order",
            "rows",
            "page",
        ]

        request_params = {
            "rows": params.get("rows", 20),
            "page": params.get("page", 1),
        }

        for key in supported_params:
            if key in ("rows", "page"):
                continue

            value = params.get(key)
            if value is not None:
                request_params[key] = value

        return request_params
    
    def _deduplicate_records(self, records):
        """
        Deduplicate OSTI records using osti_id when available.
        """

        seen = set()
        unique_records = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            key = rec.get("osti_id") or rec.get("doi") or rec.get("title")

            if key is None:
                unique_records.append(rec)
                continue

            if key not in seen:
                seen.add(key)
                unique_records.append(rec)

        return unique_records    

# ---------------------------------------------------
# API Helpers
# ---------------------------------------------------   
    def _request(self, endpoint, params=None):
        """
        Execute GET request against OSTI API.

        Parameters
        ----------
        `endpoint` : str
            OSTI API endpoint name
        `params` : dict, optional
            Query parameters for the request

        Returns
        -------
        object
            Parsed JSON response from the OSTI API.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = requests.get(
                url,
                params=params or {},
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            self._loaded = False
            raise RuntimeError(f"HTTP error while calling OSTI: {e}") from e

        except requests.exceptions.RequestException as e:
            self._loaded = False
            raise RuntimeError(f"Request failed: {e}") from e 

        except ValueError as e:
            self._loaded = False
            raise RuntimeError(f"Invalid JSON response: {e}") from e
        

    def _flatten_list(self, values, key=None):
        """
        Flatten a list of values (strings or dicts) into a semicolon-separated string.
        """

        if not values:
            return ""

        flattened = []

        for item in values:
            if isinstance(item, dict):
                value = item.get(key) if key else None
                if value is None:
                    value = item.get("name") or item.get("value") or str(item)
            else:
                value = item

            if value is not None:
                flattened.append(str(value))

        return "; ".join(flattened)
    

    def _extract_tables(self, records):
        """
            Flatten OSTI dataset JSON into records tables.

            Parameters
            ----------
            `records` : list
                List of dataset records from OSTI API

            Returns
            -------
            'record_rows' : list of dict flattened dataset metadata

        """

        record_rows = []

        for rec in records:
            authors = rec.get("authors", []) or []
            subjects = rec.get("subjects", []) or []
            links = rec.get("links", []) or []

            citation_url = None
            doe_pages_url = None
            fulltext_url = None

            for link in links:
                if isinstance(link, dict):
                    rel = link.get("rel")
                    href = link.get("href")

                    if rel == "citation":
                        citation_url = href
                    elif rel == "citation_doe_pages":
                        doe_pages_url = href
                    elif rel == "fulltext":
                        fulltext_url = href

            record_rows.append({
                "osti_id": rec.get("osti_id"),
                "title": rec.get("title"),
                "doi": rec.get("doi"),
                "publication_date": rec.get("publication_date"),
                "entry_date": rec.get("entry_date"),
                "language": rec.get("language"),
                "country_publication": rec.get("country_publication"),
                "site_ownership_code": rec.get("site_ownership_code"),
                "product_type": rec.get("product_type"),
                "description": rec.get("description"),
                "publisher": rec.get("publisher"),
                "journal_name": rec.get("journal_name"),
                "journal_volume": rec.get("journal_volume"),
                "journal_issue": rec.get("journal_issue"),
                "journal_page": rec.get("journal_page"),
                "conference_name": rec.get("conference_name"),
                "conference_location": rec.get("conference_location"),
                "conference_date": rec.get("conference_date"),
                "report_number": rec.get("report_number"),
                "contract_number": rec.get("contract_number") or rec.get("doe_contract_number"),
                "source_id": rec.get("source_id"),
                "source_osti_id": rec.get("source_osti_id"),

                "authors": self._flatten_list(authors, key="name"),
                "subjects": self._flatten_list(subjects),
                "sponsor_orgs": self._flatten_list(rec.get("sponsor_orgs", []) or [], key="name"),
                "research_orgs": self._flatten_list(rec.get("research_orgs", []) or [], key="name"),
                "other_identifiers": self._flatten_list(rec.get("other_identifiers", []) or []),

                "has_fulltext": fulltext_url is not None,
                "citation_url": citation_url,
                "citation_doe_pages_url": doe_pages_url,
                "fulltext_url": fulltext_url,

                "raw_links": links,
                "raw_record": rec,
            })

        return record_rows


    def _rows_to_table(self, rows):
        """
        Convert list-of-dicts to column-oriented OrderedDict.

        Parameters
        ----------
        `rows` : list of dict
            Row data as list of dictionaries

        Returns
        -------
        OrderedDict
            Column-oriented table structure
        """

        if not rows:
            return OrderedDict()

        # Preserve column order while capturing all keys
        cols = list(rows[0].keys())
        for r in rows[1:]:
            for k in r.keys():
                if k not in cols:
                    cols.append(k)

        table = OrderedDict({c: [] for c in cols})

        for r in rows:
            for c in cols:
                table[c].append(r.get(c))

        return table

    # ----------------------------------------------------------------------
    # Terminal Methods
    # ----------------------------------------------------------------------
    def get_table(self, table_name="records", dict_return=False):
        """
        Returns all data from the records table.
        
        Parameters
        ----------
        `table_name` : str
            Must be "records"
        `dict_return` : bool, default False
            If True, returns OrderedDict. If False, returns DataFrame.
        
        Returns
        -------
        OrderedDict or pandas.DataFrame
        """
        if table_name != "records":
            raise ValueError("OSTI backend only contains the 'records' table")

        if "records" not in self._cache:
            raise ValueError("No OSTI records loaded")

        table = self._cache["records"]

        if dict_return:
            return table

        return pd.DataFrame(table)

    def get_schema(self):
        """OSTI does not store structural schema - data comes from OSTI API."""
        return (
            "-- OSTI Backend Schema Information\n"
            "-- OSTI is a read-only REST metadata backend\n"
            "-- Data is retrieved dynamically from the API\n"
            "-- Use summary() or list() to view available tables and columns\n"
        )

    def overwrite_table(self, table_name, collection):
        """
        Not supported - OSTI backend is read-only.
        
        Parameters
        ----------
        `table_name` : str or list
            Table name(s)
        `collection` : DataFrame or list
            Data
        
        Raises
        ------
        NotImplementedError
            Always raised as OSTI is read-only
        """
        raise NotImplementedError(
            "OSTI backend is read-only. Cannot overwrite tables. "
            "To modify data, use artifact_handler('process') to load into "
            "a writable backend (Sqlite/DuckDB), make changes, then query."
        )

    # ---------------------------------------------------
    # Query Interface (in-memory)
    # ---------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query the cached records table using pandas.query().

        Parameters
        ----------
        query : str
            Pandas query string for filtering data.
        dict_return : bool, default True
            If True, returns OrderedDict format.
            If False, returns a pandas DataFrame.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        OrderedDict or pandas.DataFrame
            Query results from the records table.
        """
        if not self._loaded:
            raise RuntimeError("No metadata loaded. Cannot query empty backend.")

        if "records" not in self._cache:
            raise RuntimeError("No records table loaded.")

        df = pd.DataFrame(self._cache["records"])

        if df.empty:
            raise ValueError(f"Query returned no results: '{query}'")

        try:
            result_df = df.query(query, engine="python")

            if result_df.empty:
                raise ValueError(f"Query returned no results: '{query}'")

            if dict_return:
                return OrderedDict(result_df.to_dict(orient="list"))

            return result_df

        except pd.errors.UndefinedVariableError:
            raise ValueError(f"Query references an unknown column: '{query}'") from None

        except ValueError:
            raise

        except Exception as e:
            raise ValueError(f"Query error in records: {e}") from e

    # ----------------------------------------------------------------------
    # Artifact Processing (tiered table construction)
    # ----------------------------------------------------------------------
    def process_artifacts(self):
        """
        Returns all cached OSTI data.

        Structure:
            {
                "records": <records table>
            }

        Useful for exporting or writing to external systems.

        Returns
        -------
        OrderedDict
            Cached records table
        """
        if not self._loaded:
            return {}

        return self._cache


    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------
    def validate_urls(self):
        """
        Validate URL fields in the records table.

        Adds boolean columns indicating whether each URL is reachable:
            - citation_url_valid
            - citation_doe_pages_url_valid
            - fulltext_url_valid

        Returns
        -------
        None
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot validate URLs.")

        if "records" not in self._cache:
            raise RuntimeError("No records table loaded.")

        df = pd.DataFrame(self._cache["records"])

        if df.empty:
            return

        url_fields = [
            "citation_url",
            "citation_doe_pages_url",
            "fulltext_url",
        ]

        for field in url_fields:
            if field not in df.columns:
                continue

            validity = []

            for url in df[field]:
                if not url:
                    validity.append(False)
                    continue

                try:
                    response = requests.head(
                        url,
                        allow_redirects=True,
                        timeout=5,
                        verify=self.verify_ssl,
                    )

                    # Some servers block HEAD → fallback to GET
                    if response.status_code == 405:
                        response = requests.get(
                            url,
                            timeout=5,
                            verify=self.verify_ssl,
                        )

                    validity.append(200 <= response.status_code < 400)

                except Exception:
                    validity.append(False)

            df[f"{field}_valid"] = validity

        # write back to cache
        self._cache["records"] = df.to_dict(orient="list")


    # ----------------------------------------------------------------------
    # Find Methods
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """
        Search for query_object across table names, column names, and cell values.
        """
        if not self._loaded:
            return []

        query_str = str(query_object).lower()

        return (
            self.find_table(query_str) +
            self.find_column(query_str) +
            self.find_cell(query_object)
        )


    def find_table(self, query_object, **kwargs):
        """
        Find tables whose names contain query_object.
        """
        if not self._loaded or not isinstance(query_object, str):
            return []

        matches = []

        for table_name, table_data in self._cache.items():
            if query_object.lower() in table_name.lower():
                val = ValueObject()
                val.t_name = table_name
                val.c_name = list(table_data.keys())
                val.value = table_data
                val.type = "table"
                matches.append(val)

        return matches


    def find_column(self, query_object, **kwargs):
        """
        Find columns whose names contain query_object.
        """
        if not self._loaded or not isinstance(query_object, str):
            return []

        matches = []

        for table_name, table_data in self._cache.items():
            for col_name, col_data in table_data.items():
                if query_object.lower() in col_name.lower():
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    val.value = col_data
                    val.type = "column"
                    matches.append(val)

        return matches


    def find_cell(self, query_object, **kwargs):
        """
        Find cells that match query_object.

        Matching behavior:
            - Exact match for all data types
            - Case-insensitive partial match for strings
        """
        if not self._loaded:
            return []

        matches = []

        is_str_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str_query else None

        for table_name, table_data in self._cache.items():
            if not table_data:
                continue

            cols = list(table_data.keys())
            rows = zip(*table_data.values())

            for row_idx, row in enumerate(rows):
                for col_idx, cell in enumerate(row):
                    match = False

                    if query_object == cell:
                        match = True

                    elif (
                        is_str_query
                        and isinstance(cell, str)
                        and query_lower in cell.lower()
                    ):
                        match = True

                    if match:
                        val = ValueObject()
                        val.t_name = table_name
                        val.c_name = [cols[col_idx]]
                        val.row_num = row_idx
                        val.value = cell
                        val.type = "cell"
                        matches.append(val)

        return matches


    def find_relation(self, column_name, relation, **kwargs):
        """
        Relation finding is not supported for the OSTI backend.
        """
        return []

    # ----------------------------------------------------------------------
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        List cached OSTI tables.

        Parameters
        ----------
        collection : bool, default False
            If True, return table names.
            If False, print table names with dimensions.
        """
        if collection:
            return self._cache.keys()

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"{name}: ({len(df)} rows, {len(df.columns)} cols)")


    def summary(self, table_name=None):
        """
        Return metadata summary for the cached records table.
        """
        if not self._loaded or "records" not in self._cache:
            return pd.DataFrame()

        if table_name and table_name != "records":
            raise ValueError("OSTI backend only contains the 'records' table")

        df = pd.DataFrame(self._cache["records"])

        summary_dict = {
            "table_name": "records",
            "num_rows": len(df),
            "num_columns": len(df.columns),
            "columns": list(df.columns),
        }

        summary_df = pd.DataFrame([summary_dict])

        if table_name:
            return summary_df

        return [["records"], summary_df]


    def display(self, table_name="records", num_rows=25, display_cols=None):
        """
        Display rows from the records table.
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        if table_name != "records":
            raise ValueError("OSTI backend only contains the 'records' table")

        if "records" not in self._cache:
            raise ValueError("No OSTI records loaded")

        df = pd.DataFrame(self._cache["records"])

        if df.empty:
            raise ValueError("The records table is empty")

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in 'records': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        df.attrs["max_rows"] = len(df)

        if num_rows:
            df = df.head(num_rows)

        return df.map(
            lambda x: (
                str(x)[:60] + "..."
                if isinstance(x, str) and len(str(x)) > 60
                else x
            )
        )


    def notebook(self, **kwargs):
        """
        Notebook generation is not supported for the OSTI backend.
        """
        pass


    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    def close(self):
        """
        Reset backend state and clear cached data.
        """
        self._cache = OrderedDict()
        self._loaded = False


    # ----------------------------------------------------------------------
    # Abstract Methods
    # ----------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest is not supported for the OSTI backend.
        """
        raise NotImplementedError("OSTI backend is read-only")