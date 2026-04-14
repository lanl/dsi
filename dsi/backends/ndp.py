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
# NDP Backend (Webserver - read only)
# ---------------------------------------------------------
class NDP(Webserver):
    """
    CKAN-based web backend for querying NDP metadata in-memory
    """

    # ----------------------------
    # Initialization
    # ----------------------------
    def __init__(self,
                 base_url="https://nationaldataplatform.org/catalog",
                 api_key=None,
                 verify_ssl=False,
                 webargs=None):
        """
        Initialize backend and optionally load data from API

        base_url   : CKAN instance URL
        api_key    : optional API key
        verify_ssl : toggle SSL verification
        webargs    : initial query params (keywords, tags, etc.)
        """

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl

        # Request headers
        self.headers = {}
        if api_key:
            self.headers["Authorization"] = api_key

        # In-memory storage (DSI format)
        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False

        # Initial data load
        if webargs:
            self._load_initial_data(webargs)

    # ---------------------------------------------------
    # Initial Data Load
    # ---------------------------------------------------
    def _load_initial_data(self, kwargs):
        """
        Fetch datasets/resources from CKAN API and store in memory

        kwargs:
            keywords, organization, tags, formats, limit
        """

        params = {"rows": kwargs.get("limit", 100)}

        q_parts, fq_parts = [], []

        if kwargs.get("keywords"):
            q_parts.append(kwargs["keywords"])

        if kwargs.get("organization"):
            fq_parts.append(f"organization:{kwargs['organization']}")

        if kwargs.get("tags"):
            fq_parts += [f"tags:{t}" for t in kwargs["tags"]]

        if kwargs.get("formats"):
            fq_parts.append("(" + " OR ".join(
                [f"res_format:{f}" for f in kwargs["formats"]]) + ")")

        if q_parts:
            params["q"] = " ".join(q_parts)

        if fq_parts:
            params["fq"] = " AND ".join(fq_parts)

        result = self._request("package_search", params)

        ds_rows, rs_rows = self._extract_tables(result.get("results", []))

        self._cache["datasets"] = self._rows_to_table(ds_rows)
        self._cache["resources"] = self._rows_to_table(rs_rows)

        self._loaded = True

    # ---------------------------------------------------
    # API Helpers
    # ---------------------------------------------------
    def _request(self, endpoint, params=None):
        """
        Execute GET request against CKAN API
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
            raise RuntimeError(f"API error: {data}")

        return data["result"]

    # ---------------------------------------------------
    def _extract_tables(self, datasets):
        """
        Flatten CKAN dataset JSON into row format
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
                "tags": ",".join([t["name"] for t in ds.get("tags", [])]),
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

    # ---------------------------------------------------
    def _rows_to_table(self, rows):
        """
        Convert list-of-dicts → column-oriented OrderedDict
        """

        if not rows:
            return OrderedDict()

        cols = rows[0].keys()
        table = OrderedDict({c: [] for c in cols})

        for r in rows:
            for c in cols:
                table[c].append(r.get(c))

        return table

    # ---------------------------------------------------
    # Query Interface (in-memory)
    # ---------------------------------------------------
    def query_artifacts(self, query, queryargs=None):
        """
        Query cached tables using pandas.query()

        queryargs:
            table       : datasets | resources
            dict_return : return OrderedDict or DataFrame
        """

        queryargs = queryargs or {}

        if not self._loaded:
            return OrderedDict()

        table_name = queryargs.get("table", "datasets")
        dict_return = queryargs.get("dict_return", True)

        df = pd.DataFrame(self._cache.get(table_name, {}))

        if df.empty:
            return OrderedDict()

        try:
            result_df = df.query(query, engine="python")
            result = result_df.to_dict(orient="list")

            return {table_name: result} if dict_return else result_df

        except Exception as e:
            raise ValueError(f"Query error: {e}")

    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------
    def validate_urls(self):
        """
        Validate resource URLs (adds 'url_valid' column)
        """

        resources = self._cache.get("resources", {})
        urls = resources.get("url", [])

        valid_list = []
        headers = {"User-Agent": "NDP-Validator"}

        for url in urls:
            try:
                r = requests.head(url, allow_redirects=True,
                                  headers=headers, timeout=10,
                                  verify=self.verify_ssl)

                if r.status_code == 405:
                    r = requests.get(url, stream=True,
                                     headers=headers, timeout=10,
                                     verify=self.verify_ssl)

                valid_list.append(200 <= r.status_code < 400)

            except Exception:
                valid_list.append(False)

        resources["url_valid"] = valid_list

    # ---------------------------------------------------
    # Find Methods
    # ---------------------------------------------------
    def find(self, query_object, kwargs=None):
        """
        Search across tables, columns, and cells

        Returns:
            List[ValueObject] grouped implicitly by type:
            - table matches
            - column matches
            - cell matches
        """
        if not isinstance(query_object, str):
            return []

        query_lower = query_object.lower()

        return (
            self.find_table(query_lower) +
            self.find_column(query_lower) +
            self.find_cell(query_lower)
        )


    def find_table(self, query_object, kwargs=None):
        """Match table names"""
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


    def find_column(self, query_object, kwargs=None):
        """Match column names"""
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


    def find_cell(self, query_object, kwargs=None):
        """Match cell values"""
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

                    # exact match
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


    def find_relation(self, query_object, kwargs=None):
        """Not applicable for NDP"""
        return []


    # # ---------------------------------------------------
    # # Find Methods
    # # ---------------------------------------------------

    # def find(self, query_object, kwargs=None):
    #     """
    #     Search across tables, columns, and cells.

    #     TODO:
    #         - Consider returning a structured dict (grouped by type)
    #         instead of a flat list for better readability.
    #         - Optionally deduplicate results (same match may appear in multiple find_* calls).
    #         - Consider adding a 'limit' or 'max_results' kwarg for large datasets.
    #     """

    #     return (
    #         self.find_table(query_object) +
    #         self.find_column(query_object) +
    #         self.find_cell(query_object)
    #     )


    # def find_table(self, query_object, kwargs=None):
    #     """
    #     Match table names.

    #     TODO:
    #         - Consider returning match score (e.g. exact vs partial match).
    #         - Could optionally include table metadata (row counts, column stats).
    #         - Consider case-insensitive normalization once per call (performance improvement).
    #     """

    #     if not isinstance(query_object, str):
    #         return []

    #     matches = []

    #     for table in self._cache:
    #         if query_object.lower() in table.lower():
    #             val = ValueObject()
    #             val.t_name = table

    #             # TODO: this includes all columns; could be expensive for large tables
    #             val.c_name = list(self._cache[table].keys())

    #             val.value = self._cache[table]
    #             val.type = "table"
    #             matches.append(val)

    #     return matches


    # def find_column(self, query_object, kwargs=None):
    #     """
    #     Match column names.

    #     TODO:
    #         - Consider returning column + sample values instead of full column (memory heavy).
    #         - Could support regex or exact match modes via kwargs.
    #         - Consider grouping results by table instead of flat list.
    #     """

    #     if not isinstance(query_object, str):
    #         return []

    #     matches = []

    #     for table, data in self._cache.items():
    #         for col in data:
    #             if query_object.lower() in col.lower():
    #                 val = ValueObject()
    #                 val.t_name = table
    #                 val.c_name = [col]

    #                 # TODO: full column extraction may be expensive for large datasets
    #                 val.value = data[col]

    #                 val.type = "column"
    #                 matches.append(val)

    #     return matches


    # def find_cell(self, query_object, kwargs=None):
    #     """
    #     Match cell values.

    #     TODO:
    #         - This is the most expensive operation (O(n²) scan).
    #         - Consider caching inverted index for faster lookup.
    #         - Could add 'row return mode' like your SQLite backend.
    #         - Could support numeric tolerance / fuzzy matching.
    #     """

    #     matches = []

    #     for table, data in self._cache.items():
    #         cols = list(data.keys())

    #         # NOTE: assumes all columns have equal length
    #         # TODO: validate column length consistency on ingest
    #         rows = list(zip(*data.values()))

    #         for i, row in enumerate(rows):
    #             for j, cell in enumerate(row):

    #                 if (
    #                     query_object == cell or
    #                     (isinstance(cell, str) and
    #                     isinstance(query_object, str) and
    #                     query_object.lower() in cell.lower())
    #                 ):
    #                     val = ValueObject()
    #                     val.t_name = table
    #                     val.c_name = [cols[j]]
    #                     val.row_num = i
    #                     val.value = cell
    #                     val.type = "cell"
    #                     matches.append(val)

    #     return matches


    # def find_relation(self, query_object, kwargs=None):
    #     """
    #     Not applicable for NDP backend.

    #     TODO:
    #         - Could be extended in future if relationships/foreign keys
    #         are inferred from CKAN metadata or dataset links.
    #     """
    #     return []
    # ---------------------------------------------------
    # Utility / Display
    # ---------------------------------------------------
    def list(self, kwargs=None):
        """List available tables"""
        return list(self._cache.keys())

    def summary(self, kwargs=None):
        """Return basic table summary"""
        return {
            "loaded": self._loaded,
            "tables": {
                name: len(next(iter(table.values()), []))
                for name, table in self._cache.items()
            }
        }

    def display(self, kwargs=None):
        """Print preview of tables"""
        for name, table in self._cache.items():
            print(f"\n{name}:")
            print(pd.DataFrame(table).head())

    def notebook(self, kwargs=None):
        """Notebook generation not supported"""
        pass

    # ---------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------
    def close(self):
        """Reset backend state"""
        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })
        self._loaded = False


    # # ---------------------------------------------------
    # # Utility / Display (SQLite-style refactor)
    # # ---------------------------------------------------

    # def list(self, kwargs=None):
    #     """
    #     List available tables in memory.

    #     SQLite-style behavior:
    #         - returns raw list of table names
    #         - no printing / formatting
    #     """

    #     kwargs = kwargs or {}

    #     # TODO: support filtering like:
    #     # kwargs = {"pattern": "dataset"}
    #     pattern = kwargs.get("pattern")

    #     tables = list(self._cache.keys())

    #     if pattern and isinstance(pattern, str):
    #         tables = [t for t in tables if pattern.lower() in t.lower()]

    #     return tables


    # def summary(self, kwargs=None):
    #     """
    #     Return dataset/table summary.

    #     SQLite-style behavior:
    #         - returns structured metadata (not printed)
    #         - safe for programmatic use
    #     """

    #     kwargs = kwargs or {}
    #     table_filter = kwargs.get("table")

    #     summary = {
    #         "loaded": self._loaded,
    #         "tables": {}
    #     }

    #     for name, table in self._cache.items():

    #         if table_filter and table_filter != name:
    #             continue

    #         # NOTE: assumes column lengths are consistent
    #         row_count = len(next(iter(table.values()), [])) if table else 0

    #         summary["tables"][name] = {
    #             "rows": row_count,
    #             "columns": len(table.keys()) if table else 0
    #         }

    #     return summary


    # def display(self, kwargs=None):
    #     """
    #     Display table previews.

    #     SQLite-style behavior:
    #         - returns DataFrame(s), NOT print output
    #         - user/UI layer decides how to render
    #     """

    #     kwargs = kwargs or {}

    #     table_name = kwargs.get("table")
    #     limit = kwargs.get("limit", 5)

    #     results = {}

    #     for name, table in self._cache.items():

    #         if table_name and name != table_name:
    #             continue

    #         df = pd.DataFrame(table)

    #         # Apply preview limit (like SQLite .head())
    #         results[name] = df.head(limit)

    #     # If only one table requested, return DataFrame directly
    #     if table_name:
    #         return results.get(table_name, pd.DataFrame())

    #     return results


    # def notebook(self, kwargs=None):
    #     """
    #     Notebook generation (placeholder).

    #     SQLite-style expectation:
    #         - should eventually return a notebook object or file path
    #         - NOT print anything

    #     TODO:
    #         - Generate Jupyter notebook (nbformat)
    #         - Each table becomes a DataFrame cell
    #         - Include summary cell at top
    #     """
    #     pass
    
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Not supported for NDP (read-only backend)
        """
        raise NotImplementedError("NDP backend is read-only")