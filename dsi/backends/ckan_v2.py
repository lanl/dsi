#!/usr/bin/env python3
"""
Clean CKAN Backend for DSI

Provides read-only access to CKAN catalogs and exposes metadata
as DSI tables: datasets and resources.
"""

import requests
import pandas as pd
from collections import OrderedDict
from dsi.backends.webserver import Webserver
from dsi.core import ValueObject


class CKAN(Webserver):

    def __init__(self,
                 base_url="https://nationaldataplatform.org/catalog",
                 api_key=None,
                 verify_ssl=False):

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.verify_ssl = verify_ssl

        self.headers = {}
        if api_key:
            self.headers["Authorization"] = api_key

        # cached tables
        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False

    # ---------------------------------------------------
    # Internal API
    # ---------------------------------------------------

    def _request(self, endpoint, params=None):

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

    def _extract_tables(self, datasets):

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

    def _rows_to_table(self, rows):

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

    def ingest_artifacts(self, artifacts, kwargs):
        """
        Load metadata from CKAN.
        """

        keywords = kwargs.get("keywords")
        organization = kwargs.get("organization")
        tags = kwargs.get("tags")
        formats = kwargs.get("formats")
        limit = kwargs.get("limit", 100)

        params = {"rows": limit}

        q_parts = []
        fq_parts = []

        if keywords:
            q_parts.append(keywords)

        if organization:
            fq_parts.append(f"organization:{organization}")

        if tags:
            for t in tags:
                fq_parts.append(f"tags:{t}")

        if formats:
            fq_parts.append("(" + " OR ".join(
                [f"res_format:{f}" for f in formats]) + ")")

        if q_parts:
            params["q"] = " ".join(q_parts)

        if fq_parts:
            params["fq"] = " AND ".join(fq_parts)

        result = self._request("package_search", params)

        datasets = result.get("results", [])

        ds_rows, rs_rows = self._extract_tables(datasets)

        self._cache["datasets"] = self._rows_to_table(ds_rows)
        self._cache["resources"] = self._rows_to_table(rs_rows)

        self._loaded = True

    # ---------------------------------------------------

    def process_artifacts(self, kwargs):

        if not self._loaded:
            return OrderedDict()

        return self._cache

    # ---------------------------------------------------

    def query_artifacts(self, query, kwargs):

        if not self._loaded:
            return (RuntimeError, "No metadata loaded")

        dict_return = kwargs.get("dict_return", False)

        if "resource" in query or "dataset_id" in query:
            df = pd.DataFrame(self._cache["resources"])
        else:
            df = pd.DataFrame(self._cache["datasets"])

        try:

            result = df.query(query)

            if dict_return:
                return OrderedDict(result.to_dict(orient="list"))

            return result

        except Exception as e:
            return (ValueError, str(e))

    # ---------------------------------------------------
    # Find Functions
    # ---------------------------------------------------

    def find(self, query_object, kwargs):

        results = []

        results += self.find_table(query_object, kwargs) or []
        results += self.find_column(query_object, kwargs) or []
        results += self.find_cell(query_object, kwargs) or []

        return results

    def find_table(self, query_object, kwargs):

        if not isinstance(query_object, str):
            return None

        matches = []

        for table in self._cache.keys():

            if query_object.lower() in table.lower():

                v = ValueObject()
                v.t_name = table
                v.c_name = list(self._cache[table].keys())
                v.value = None
                v.type = "table"

                matches.append(v)

        return matches

    def find_column(self, query_object, kwargs):

        if not isinstance(query_object, str):
            return None

        matches = []

        for table, data in self._cache.items():

            for col in data.keys():

                if query_object.lower() in col.lower():

                    v = ValueObject()
                    v.t_name = table
                    v.c_name = [col]
                    v.value = data[col]
                    v.type = "column"

                    matches.append(v)

        return matches

    def find_cell(self, query_object, kwargs):

        matches = []

        for table, data in self._cache.items():

            cols = list(data.keys())

            rows = list(zip(*data.values()))

            for i, row in enumerate(rows):

                for j, cell in enumerate(row):

                    if query_object == cell or (
                        isinstance(cell, str)
                        and isinstance(query_object, str)
                        and query_object.lower() in cell.lower()
                    ):

                        v = ValueObject()
                        v.t_name = table
                        v.c_name = [cols[j]]
                        v.row_num = i
                        v.value = cell
                        v.type = "cell"

                        matches.append(v)

        return matches

    # ---------------------------------------------------
    # Notebook
    # ---------------------------------------------------

    def notebook(self, kwargs):

        interactive = kwargs.get("interactive", False)

        df_datasets = pd.DataFrame(self._cache["datasets"])
        df_resources = pd.DataFrame(self._cache["resources"])

        print("Datasets:")
        print(df_datasets.head())

        print("\nResources:")
        print(df_resources.head())

    # ---------------------------------------------------

    def close(self):

        self._cache = OrderedDict({
            "datasets": OrderedDict(),
            "resources": OrderedDict()
        })

        self._loaded = False