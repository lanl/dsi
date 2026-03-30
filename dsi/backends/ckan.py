#!/usr/bin/env python3
"""
CKAN Backend for DSI

Provides read-only access to CKAN catalogs and exposes metadata
as DSI tables: datasets and resources.
"""

import requests
import pandas as pd
from collections import OrderedDict

from dsi.backends.webserver import Webserver

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------
# ValueObject (DSI Standard)
# ---------------------------------------------------

class ValueObject:
    """
    Data Structure used when returning search results from find functions

    - t_name: table name
    - c_name: column name(s)
    - row_num: row index
    - value: matched value
    - type: match type {table, column, cell}
    """
    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""


# ---------------------------------------------------
# CKAN Backend
# ---------------------------------------------------

class CKAN(Webserver):
    """
    CKAN Web Backend (READ-ONLY)

    Loads CKAN datasets via API and exposes them as:
        - datasets table
        - resources table

    Supports:
        - ingest_artifacts (API fetch)
        - query_artifacts (pandas query)
        - find (table/column/cell search)
    """

    # ---------------------------------------------
    # Initialization
    # ---------------------------------------------

    def __init__(self,
                 base_url="https://nationaldataplatform.org/catalog",
                 api_key=None,
                 verify_ssl=False):
        """
        Initialize CKAN backend

        `base_url` : str
            CKAN instance base URL

        `api_key` : str, optional
            API key for authenticated endpoints

        `verify_ssl` : bool
            Toggle SSL verification
        """

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl

        self.headers = {}
        if api_key:
            self.headers["Authorization"] = api_key

        # Internal storage
        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False

    # ---------------------------------------------
    # Internal API Helpers
    # ---------------------------------------------

    def _request(self, endpoint, params=None):
        """
        Internal helper for CKAN API requests
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
            raise RuntimeError(f"CKAN API error: {data}")

        return data["result"]

    # ---------------------------------------------

    def _extract_tables(self, datasets):
        """
        Convert CKAN dataset JSON into row format
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

    # ---------------------------------------------

    def _rows_to_table(self, rows):
        """
        Convert list-of-dicts → OrderedDict (DSI format)
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
    # DSI Interface
    # ---------------------------------------------------

    def ingest_artifacts(self, artifacts=None, kwargs=None):
        """
        Fetch metadata from CKAN API

        `kwargs` supports:
            - keywords
            - organization
            - tags
            - formats
            - limit
        """

        kwargs = kwargs or {}

        params = {"rows": kwargs.get("limit", 100)}

        if "keywords" in kwargs:
            params["q"] = kwargs["keywords"]

        result = self._request("package_search", params)
        datasets = result.get("results", [])

        ds_rows, rs_rows = self._extract_tables(datasets)

        self._cache["datasets"] = self._rows_to_table(ds_rows)
        self._cache["resources"] = self._rows_to_table(rs_rows)

        self._loaded = True

    # ---------------------------------------------------

    def process_artifacts(self, kwargs=None):
        """
        Return loaded data in DSI format
        """
        if not self._loaded:
            return OrderedDict()
        return self._cache

    # ---------------------------------------------------

    def query_artifacts(self, query, kwargs=None):
        """
        Query cached tables using pandas.query()

        FIX:
        - Always returns DataFrame OR OrderedDict
        - No tuple-based error returns
        """

        if not self._loaded:
            raise RuntimeError("No metadata loaded")

        kwargs = kwargs or {}
        dict_return = kwargs.get("dict_return", False)

        # choose table
        if "resource" in query.lower():
            df = pd.DataFrame(self._cache["resources"])
        else:
            df = pd.DataFrame(self._cache["datasets"])

        result = df.query(query)

        if dict_return:
            return OrderedDict(result.to_dict(orient="list"))

        return result

    # ---------------------------------------------------
    # FIND FUNCTIONS
    # ---------------------------------------------------

    def find(self, query_object, kwargs=None):
        """
        Search across tables, columns, and cells
        """
        results = []
        results += self.find_table(query_object) or []
        results += self.find_column(query_object) or []
        results += self.find_cell(query_object) or []
        return results

    def find_table(self, query_object):
        """
        Match table names
        """
        if not isinstance(query_object, str):
            return None

        matches = []

        for table in self._cache.keys():
            if query_object.lower() in table.lower():
                vo = ValueObject()
                vo.t_name = table
                vo.c_name = list(self._cache[table].keys())
                vo.type = "table"
                matches.append(vo)

        return matches

    def find_column(self, query_object):
        """
        Match column names
        """
        if not isinstance(query_object, str):
            return None

        matches = []

        for table, data in self._cache.items():
            for col in data.keys():
                if query_object.lower() in col.lower():
                    vo = ValueObject()
                    vo.t_name = table
                    vo.c_name = [col]
                    vo.value = data[col]
                    vo.type = "column"
                    matches.append(vo)

        return matches

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
                        (isinstance(cell, str) and isinstance(query_object, str)
                         and query_object.lower() in cell.lower())
                    ):
                        vo = ValueObject()
                        vo.t_name = table
                        vo.c_name = [cols[j]]
                        vo.row_num = i
                        vo.value = cell
                        vo.type = "cell"
                        matches.append(vo)

        return matches

    # ---------------------------------------------------

    def notebook(self, kwargs=None):
        """
        Simple preview of loaded tables
        """
        print(pd.DataFrame(self._cache["datasets"]).head())
        print(pd.DataFrame(self._cache["resources"]).head())

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
        return "ckan-readonly-backend"

    def get_artifacts(self, kwargs=None):
        return self._cache

    def read_to_artifacts(self, kwargs=None):
        return self._cache

    def inspect_artifacts(self, kwargs=None):
        return {
            "loaded": self._loaded,
            "tables": list(self._cache.keys())
        }

    def put_artifacts(self, artifacts, kwargs=None):
        raise NotImplementedError("CKAN backend is read-only.")