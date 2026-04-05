"""
NDP-CKAN Webserver Backend for DSI

Provides read-only access to NDP (CKAN-based) catalogs via API and exposes metadata
as DSI-compatible tables: datasets and resources.

This backend DOES NOT write data — it only queries remote NDP-CKAN instances.
"""

import requests
import pandas as pd
from collections import OrderedDict
from urllib.parse import urlparse

from dsi.backends.webserver import Webserver

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------
# ValueObject
# ---------------------------------------------------------
class ValueObject:
    """
    Data structure used when returning search results from:
    find(), find_table(), find_column(), find_cell()

    Attributes:
        t_name  : table name
        c_name  : list of column names
        row_num : row index (if applicable)
        value   : matched value or data
        type    : {table, column, cell}
    """
    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""


# ---------------------------------------------------------
# NDP-CKAN Backend
# ---------------------------------------------------------
class NDP(Webserver):
    """
    NDP-CKAN Web Backend for DSI

    Converts NDP (CKAN-based) API responses into in-memory tabular format
    compatible with DSI (OrderedDict of columns).
    """

    # ----------------------------
    # Constructor
    # ----------------------------
    def __init__(self,
                 base_url="https://nationaldataplatform.org/catalog",
                 api_key=None,
                 verify_ssl=False):
        """
        Initialize NDP-CKAN backend connection settings

        Parameters:
            base_url   : NDP-CKAN instance base URL
            api_key    : optional API key
            verify_ssl : SSL verification flag
        """

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid NDP-CKAN base_url")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl

        # HTTP headers
        self.headers = {}
        if api_key:
            self.headers["Authorization"] = api_key

        # Internal storage
        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False

    # ---------------------------------------------------
    # NDP-CKAN API Helpers
    # ---------------------------------------------------

    def _request(self, endpoint, params=None):
        """
        **Internal use only**

        Sends GET request to NDP-CKAN API and returns JSON result

        Raises:
            RuntimeError if API response is unsuccessful
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
            raise RuntimeError(f"NDP-CKAN API error: {data}")

        return data["result"]

    # ---------------------------------------------------

    def _extract_tables(self, datasets):
        """
        Convert NDP-CKAN dataset JSON into flat row structures

        Returns:
            dataset_rows  : list of dataset dicts
            resource_rows : list of resource dicts
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
        Convert list-of-dicts into column-oriented OrderedDict
        (DSI standard format)
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
    # DSI Backend Interface
    # ---------------------------------------------------

    def query_artifacts(self, query=None, kwargs=None):
        """
        Fetch metadata from NDP-CKAN and store internally

        kwargs options:
            keywords, organization, tags, formats, limit
        """

        kwargs = kwargs or {}

        params = {"rows": kwargs.get("limit", 100)}

        # Build query filters
        q_parts = []
        fq_parts = []

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

        # API call
        result = self._request("package_search", params)
        datasets = result.get("results", [])

        # Transform
        ds_rows, rs_rows = self._extract_tables(datasets)

        self._cache["datasets"] = self._rows_to_table(ds_rows)
        self._cache["resources"] = self._rows_to_table(rs_rows)

        self._loaded = True

    # ---------------------------------------------------

    def process_artifacts(self, kwargs=None):
        """
        Return cached data in DSI format
        """
        return self._cache if self._loaded else OrderedDict()

    # ---------------------------------------------------

    def query_in_memory(self, query, kwargs=None):
        """
        Query in-memory cached data using pandas.query()

        kwargs:
            table       : "datasets" or "resources"
            dict_return : always return OrderedDict if True
        """
        kwargs = kwargs or {}

        if not self._loaded:
            return OrderedDict()

        table_name = kwargs.get("table", "datasets")
        dict_return = kwargs.get("dict_return", True)

        # normalize table to column-oriented OrderedDict
        rows = self._cache.get(table_name, [])

        if isinstance(rows, list):
            # convert list-of-dicts to column-oriented OrderedDict
            if rows:
                cols = rows[0].keys()
                table_od = OrderedDict({c: [] for c in cols})
                for r in rows:
                    for c in cols:
                        table_od[c].append(r.get(c))
            else:
                table_od = OrderedDict()
        elif isinstance(rows, dict) or isinstance(rows, OrderedDict):
            table_od = OrderedDict(rows)
        else:
            table_od = OrderedDict()

        df = pd.DataFrame(table_od)

        try:
            result_df = df.query(query, engine="python")
            return OrderedDict(result_df.to_dict(orient="list")) if dict_return else result_df.to_dict(orient="list")
        except Exception as e:
            raise ValueError(f"Query error: {e}")

    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------

    def validate_urls(self):
        """
        Check each resource URL in the 'resources' table and
        add a column 'url_valid' indicating whether the URL can be reached.

        Updates:
            self._cache["resources"]["url_valid"] : list of bool
        """
        resources = self._cache.get("resources", {})
        urls = resources.get("url", [])
        valid_list = []

        for url in urls:
            try:
                r = requests.head(url, allow_redirects=True, timeout=5)
                valid_list.append(r.status_code == 200)
            except:
                valid_list.append(False)

        resources["url_valid"] = valid_list

    # ---------------------------------------------------
    # Find Functions
    # ---------------------------------------------------

    def find(self, query_object, kwargs=None):
        """
        Search across tables, columns, and cells
        """
        return (
            (self.find_table(query_object) or []) +
            (self.find_column(query_object) or []) +
            (self.find_cell(query_object) or [])
        )

    # ---------------------------------------------------

    def find_table(self, query_object):
        """
        Match table names
        """

        if not isinstance(query_object, str):
            return []

        matches = []

        for table in self._cache:
            if query_object.lower() in table.lower():
                val = ValueObject()
                val.t_name = table
                val.c_name = list(self._cache[table].keys())
                val.value = self._cache[table]
                val.type = "table"
                matches.append(val)

        return matches

    # ---------------------------------------------------

    def find_column(self, query_object):
        """
        Match column names
        """

        if not isinstance(query_object, str):
            return None

        matches = []

        for table, data in self._cache.items():
            for col in data:
                if query_object.lower() in col.lower():
                    val = ValueObject()
                    val.t_name = table
                    val.c_name = [col]
                    val.value = data[col]
                    val.type = "column"
                    matches.append(val)

        return matches or None

    # ---------------------------------------------------

    def find_cell(self, query_object):
        """
        Match cell values
        """

        matches = []

        for table, data in self._cache.items():
            cols = list(data.keys())
            rows = list(zip(*data.values()))

            for i, row in enumerate(rows):
                for j, cell in enumerate(row):

                    if (
                        query_object == cell or
                        (isinstance(cell, str) and
                         isinstance(query_object, str) and
                         query_object.lower() in cell.lower())
                    ):
                        val = ValueObject()
                        val.t_name = table
                        val.c_name = [cols[j]]
                        val.row_num = i
                        val.value = cell
                        val.type = "cell"
                        matches.append(val)

        return matches or None

    # ---------------------------------------------------
    # Notebook / Inspection
    # ---------------------------------------------------

    def notebook(self, kwargs=None):
        """
        Simple preview of loaded data
        """

        print("\nDatasets:")
        print(pd.DataFrame(self._cache["datasets"]).head())

        print("\nResources:")
        print(pd.DataFrame(self._cache["resources"]).head())

    # ---------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------

    def close(self):
        """
        Reset backend state
        """

        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False

    # ---------------------------------------------------
    # Required Abstract Methods
    # ---------------------------------------------------

    def git_commit_sha(self, kwargs=None):
        return "ndp-ckan-readonly-backend"

    def get_artifacts(self, kwargs=None):
        return self._cache

    def read_to_artifacts(self, kwargs=None):
        return self._cache

    def inspect_artifacts(self, kwargs=None):
        return {
            "loaded": self._loaded,
            "tables": list(self._cache.keys())
        }
    
    def ingest_artifacts(self, artifacts, kwargs=None):
        raise NotImplementedError("NDP-CKAN backend is read-only.")
    
    def put_artifacts(self, artifacts, kwargs=None):
        raise NotImplementedError("NDP-CKAN backend is read-only.")