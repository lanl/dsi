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

        `params`:
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
        Flatten CKAN dataset JSON into:
            - dataset_rows (list of dicts)
            - resource_map (dict: dataset_name -> list of resource rows)
        """

        dataset_rows = []
        resource_map = {}

        for ds in datasets:
            dataset_name = ds.get("name") or ds.get("id")

            dataset_rows.append({
                "id": ds.get("id"),
                "name": dataset_name,
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

            resource_map.setdefault(dataset_name, [])

            for r in ds.get("resources", []):
                resource_map[dataset_name].append({
                    "resource_id": r.get("id"),
                    "resource_name": r.get("name"),
                    "format": r.get("format"),
                    "size": r.get("size"),
                    "url": r.get("url"),
                    "dataset_id": ds.get("id"),
                    "dataset_title": ds.get("title")
                })

        return dataset_rows, resource_map


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

        `dict_return` :
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

            except pd.errors.UndefinedVariableError:
                # Skip tables that don't have the queried columns
                continue
            except Exception as e:
                raise ValueError(f"Query error in {t_name}: {e}")

        return results


    # ---------------------------------------------------
    # Artifact Processing (tiered table construction)
    # ---------------------------------------------------
    def process_artifacts(self):
        """
        Returns all cached tables in tiered format.

        Structure:
            {
                "datasets": <dataset table>,
                "<dataset_name>": <resource table>,
                ...
            }

        Useful for exporting or writing to external systems.
        """

        if not self._loaded:
            return {}

        return self._cache


    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------
    def validate_urls(self):
        """
        Validates resource URLs across all resource tables.
        Adds 'url_valid' column.
        """

        headers = {"User-Agent": "NDP-Validator"}

        for table_name in self._resource_tables:
            table = self._cache.get(table_name, {})
            urls = table.get("url", [])

            valid_list = []

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

            table["url_valid"] = valid_list


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
    def list(self, collection=False):
        """
        Lists tables or prints metadata.

        collection : bool, default False
            True  → return list of table names
            False → print table names + dimensions
        """

        if collection:
            return {
                "datasets": ["datasets"],
                "resources": self._resource_tables
            }

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"{name}: ({len(df)} rows, {len(df.columns)} cols)")
    

    def summary(self, table_name=None):
        """
        Returns numerical metadata for tables.

        `table_name` : str, optional
            If provided → returns summary for a single table
            If None → returns summary for all tables as one DataFrame
        """

        if not self._loaded:
            return pd.DataFrame()

        summaries = []

        for name, table in self._cache.items():

            if table_name and name != table_name:
                continue

            df = pd.DataFrame(table)

            summaries.append({
                "table_name": name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns)
            })

        return pd.DataFrame(summaries)
        
        
    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Displays rows from a specified table.

        `table_name` : str
            Name of the table.

        `num_rows` : int, default 25
            Number of rows to display.

        `display_cols` : list of str, optional
            Subset of columns to display.

        `return` :
            pandas.DataFrame
        """

        table = self._cache.get(table_name)

        if not table:
            raise ValueError(f"Table '{table_name}' not found")

        df = pd.DataFrame(table)

        if display_cols:
            df = df[display_cols]

        result = df.head(num_rows)
        print(result)
        return result


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
        """

        self._cache = OrderedDict()
        self._resource_tables = []
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