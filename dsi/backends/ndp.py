"""
NDP-CKAN Webserver Backend for DSI

Read-only backend that pulls metadata from CKAN-based NDP instances
and exposes it as in-memory DSI tables: datasets and resources.
"""

import requests
import pandas as pd
from collections import OrderedDict
from urllib.parse import urlparse

from dsi.backends.webserver import Webserver

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------
# Value Object (used for search results)
# ---------------------------------------------------------
class ValueObject:
    """
    Container for search results returned by find* methods

    Attributes:
        t_name   : table name
        c_name   : column name(s)
        row_num  : row index (if applicable)
        value    : matched value
        type     : {table, column, cell}
    """
    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""


# ---------------------------------------------------------
# NDP Backend (Webserver - Read only)
# ---------------------------------------------------------
class NDP(Webserver):
    """
    CKAN-based web backend for querying NDP metadata in-memory
    """

    # ----------------------------
    # Initialization
    # ----------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from CKAN API.

        url :
            Base CKAN URL. If None, a default CKAN endpoint is used.

        params :
            Dictionary of initial query parameters used to fetch data from CKAN.
            Supported keys:
                - keywords
                - organization
                - tags
                - formats
                - limit

        kwargs :
            api_key    : optional API key
            verify_ssl : toggle SSL verification (default False)
        """

        DEFAULT_URL = "https://nationaldataplatform.org/catalog"

        base_url = url or DEFAULT_URL

        # ----------------------------
        # Auth / connection config
        # ----------------------------
        self.api_key = kwargs.get("api_key")
        self.verify_ssl = kwargs.get("verify_ssl", False)

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")

        self.headers = {}
        if self.api_key:
            self.headers["Authorization"] = self.api_key

        # Data storage (tiered structure)
        # Tier 1: datasets
        # Tier 2: per-dataset resource tables
        self._cache = OrderedDict()
        self._resource_tables = []

        self._loaded = False
        self.params = params or {}

        # Initial data load
        if self.params:
            self._load_initial_data(self.params)
   
   
    # ---------------------------------------------------
    # Initial Data Load
    # ---------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch datasets/resources from CKAN API and store in memory.

        params:
            keywords, organization, tags, formats, limit
        """

        query_params = {"rows": params.get("limit", 100)}

        q_parts, fq_parts = [], []

        if params.get("keywords"):
            q_parts.append(params["keywords"])

        if params.get("organization"):
            fq_parts.append(f"organization:{params['organization']}")

        if params.get("tags"):
            fq_parts += [f"tags:{t}" for t in params["tags"]]

        if params.get("formats"):
            fq_parts.append("(" + " OR ".join(
                [f"res_format:{f}" for f in params["formats"]]) + ")")

        if q_parts:
            query_params["q"] = " ".join(q_parts)

        if fq_parts:
            query_params["fq"] = " AND ".join(fq_parts)

        result = self._request("package_search", query_params)

        dataset_rows, resource_map = self._extract_tables(result.get("results", []))

        # Tier 1: datasets
        self._cache["datasets"] = self._rows_to_table(dataset_rows)

        # Tier 2: per-dataset resource tables
        self._resource_tables = []
        for dataset_name, rows in resource_map.items():
            table_name = f"resources_{dataset_name}"
            self._cache[table_name] = self._rows_to_table(rows)
            self._resource_tables.append(table_name)

        self._loaded = True


    # ---------------------------------------------------
    # API Helpers
    # ---------------------------------------------------
    def _request(self, endpoint, params=None):
        """
        Execute GET request against CKAN API.
        """

        url = f"{self.base_url}/api/3/action/{endpoint}"

        r = requests.get(
            url,
            params=params,
            headers=self.headers,
            verify=self.verify_ssl
        )

        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            raise RuntimeError(f"CKAN API failure at {endpoint}: {data}")

        return data["result"]


    def _extract_tables(self, datasets):
        """
        Flatten CKAN dataset JSON into row format.
        """

        dataset_rows = []
        resource_rows = []

        for ds in datasets:
            dataset_rows.append({
                "id": ds.get("id"),
                "name": ds.get("name"),
                "title": ds.get("title"),
                "notes": ds.get("notes"),
                "organization": (ds.get("organization") or {}).get("title"),
                "author": ds.get("author"),
                "license": ds.get("license_title"),
                "created": ds.get("metadata_created"),
                "modified": ds.get("metadata_modified"),
                "tags": ",".join(t["name"] for t in ds.get("tags", [])),
                "num_resources": ds.get("num_resources", 0)
            })

            for r in ds.get("resources", []):
                resource_rows.append({
                    "resource_id": r.get("id"),
                    "resource_name": r.get("name"),
                    "format": r.get("format"),
                    "size": r.get("size"),
                    "url": r.get("url"),
                    "dataset_id": ds.get("id"),
                    "dataset_title": ds.get("title")
                })

        return dataset_rows, resource_rows


    def _rows_to_table(self, rows):
        """
        Convert list-of-dicts → column-oriented OrderedDict.
        """

        if not rows:
            return OrderedDict()

        cols = list(rows[0].keys())
        table = OrderedDict({c: [] for c in cols})

        for r in rows:
            for c in cols:
                table[c].append(r.get(c))

        return table


    # ---------------------------------------------------
    # Query Interface (in-memory)
    # ---------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query all tables using a pandas query string.

        dict_return :
            If True, returns dict format (default).
            If False, returns pandas DataFrames.
        """

        if not self._loaded:
            return {}

        results = {}

        for t_name, table in self._cache.items():
            df = pd.DataFrame(table)

            if df.empty:
                continue

            try:
                result_df = df.query(query, engine="python")

                if not result_df.empty:
                    results[t_name] = (
                        result_df.to_dict(orient="list")
                        if dict_return else result_df
                    )

            except Exception as e:
                raise ValueError(f"Query error in {t_name}: {e}")

        return results


    # ---------------------------------------------------
    # 
    # ---------------------------------------------------
    def process_artifacts(self):
        """
        Returns all tables (datasets + per-dataset resource tables)

        Useful for writing/exporting.
        """

        if not self._loaded:
            return {}

        return self._cache


    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------
    def validate_urls(self):
        """
        Validates resource URLs and updates cache in-place.

        Adds:
            - 'url_valid' column to the resources table indicating URL reachability.

        `return` : None
        """

        resources = self._cache["resources"]
        urls = resources.get("url", [])

        valid_list = []
        headers = {"User-Agent": "NDP-Validator"}

        for url in urls:
            try:
                r = requests.head(
                    url,
                    allow_redirects=True,
                    headers=headers,
                    timeout=10,
                    verify=self.verify_ssl
                )

                if r.status_code == 405:
                    r = requests.get(
                        url,
                        stream=True,
                        headers=headers,
                        timeout=10,
                        verify=self.verify_ssl
                    )

                valid_list.append(200 <= r.status_code < 400)

            except Exception:
                valid_list.append(False)

        resources["url_valid"] = valid_list


    # ---------------------------------------------------
    # Find Methods
    # ---------------------------------------------------
    def find(self, query_object, **kwargs):
        """
        Searches for all instances of `query_object` across all tables at the table, column, and cell levels.

        `query_object` : int, float, or str
            The value to search for across all tables in the backend.

        `return` : list of ValueObjects
            A list of ValueObjects representing matches across:
                - table names
                - column names
                - cell values

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   column name(s) (list)
            - row_num:  row index (int or None)
            - value:    matched value or data
            - type:     {'table', 'column', 'cell'}
        """
        
        query_str = str(query_object).lower()

        return (
            self.find_table(query_str) +
            self.find_column(query_str) +
            self.find_cell(query_object)
        )


    def find_table(self, query_object, **kwargs):
        """
        Finds all tables whose names contain the given `query_object` (case-insensitive).

        `query_object` : str
            The string to match against table names.

        `return` : list of ValueObjects
            One ValueObject per matching table.

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list of all columns in the table
            - value:    full table data (dict of columns)
            - row_num:  None
            - type:     'table'
        """

        if not isinstance(query_object, str):
            return []

        matches = []

        for table_name, table_data in self._cache.items():
            if query_object in table_name.lower():
                val = ValueObject()
                val.t_name = table_name
                val.c_name = list(table_data.keys())
                val.value = table_data
                val.type = "table"


                matches.append(val)

        return matches


    def find_column(self, query_object, **kwargs):
        """
        Finds all columns whose names contain the given `query_object` (case-insensitive).

        `query_object` : str
            The string to match against column names.

        `return` : list of ValueObjects
            One ValueObject per matching column.

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list containing the matched column name
            - value:    full column data (list)
            - row_num:  None
            - type:     'column'
        """

        if not isinstance(query_object, str):
            return []

        matches = []

        for table_name, table_data in self._cache.items():
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
        Finds all cells that match the given `query_object`.

        Matching behavior:
            - Exact match for all data types
            - Case-insensitive partial match for strings

        `query_object` : int, float, or str
            The value to search for within table cells.

        `return` : list of ValueObjects
            One ValueObject per matching cell.

        ValueObject Structure:
            - t_name:   table name (str)
            - c_name:   list containing the matched column name
            - row_num:  row index of the match (int)
            - value:    matched cell value
            - type:     'cell'
        """

        matches = []

        is_str_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str_query else None

        for table_name, table_data in self._cache.items():

            if not table_data:
                continue

            cols = list(table_data.keys())
            rows = zip(*table_data.values())  # avoids creating full list

            for row_idx, row in enumerate(rows):
                for col_idx, cell in enumerate(row):

                    match = False

                    # Exact match
                    if query_object == cell:
                        match = True

                    # string partial match
                    elif (
                        is_str_query and
                        isinstance(cell, str) and
                        query_lower in cell.lower()
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
        Not supported for NDP backend.

        NDP is a read-only metadata backend and does not support
        relational queries on columns.

        `return` : list
            Always returns an empty list.
        """
        return []
    
    # ---------------------------------------------------
    # Utility / Display
    # ---------------------------------------------------
    def list(self, **kwargs):
        """
        Lists all available tables in the backend.

        `return` : list of str
            A list of table names currently stored in memory.
        """
        return list(self._cache.keys())
    

    def summary(self, table_name, **kwargs):
        """
        Returns summary information about tables in the backend.

        table_name :
            Optional table name. If provided, returns summary only for that table.

        return :
            dict containing:
                - loaded: bool
                - tables: dict mapping table names to row counts
        """

        tables_summary = {}

        for name, table in self._cache.items():

            if table_name is not None and name != table_name:
                continue

            row_count = len(next(iter(table.values()), [])) if table else 0
            tables_summary[name] = row_count

        return {
            "loaded": self._loaded,
            "tables": tables_summary
        }
        

    def display(self, table_name=None, **kwargs):
        """
        Displays a preview of table data.

        table_name :
            Optional table name. If provided, displays only that table.

        kwargs :
            limit : number of rows to display per table (default 10)

        return :
            None (prints DataFrame previews)
        """

        limit = kwargs.get("limit", 10)

        for name, table in self._cache.items():

            if table_name is not None and name != table_name:
                continue

            print(f"\n{name}:")
            df = pd.DataFrame(table)
            print(df.head(limit))


    def notebook(self, **kwargs):
        """
        Notebook generation not supported for NDP backend.

        `return` : None
        """
        pass


    # ---------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------
    def close(self):
        """
        Resets backend state and clears all cached data.

        `return` : None
        """

        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })
        self._loaded = False


    # ---------------------------------------------------
    # Abstract Methods
    # ---------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest not supported for NDP backend (read-only).

        `return` : None
        """
        raise NotImplementedError("NDP backend is read-only")