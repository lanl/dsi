"""
NDP-CKAN Webserver Backend for DSI

Read-only backend that pulls metadata from CKAN-based NDP instances
and exposes it as in-memory DSI tables: datasets and resources.
"""

import requests
import pandas as pd
import numpy as np
from collections import OrderedDict
from urllib.parse import urlparse

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


# ----------------------------------------------------------------------
# NDP Backend (Webserver - Read only)
# ----------------------------------------------------------------------
class NDP(Webserver):
    """
    CKAN-based web backend for querying NDP metadata in-memory
    """

    # ----------------------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from CKAN API.

        Parameters
        ----------
        `url` : str, optional
            Base CKAN URL. If None, a default CKAN endpoint is used.
        `params` : dict, optional
            Dictionary of initial query parameters used to fetch data from CKAN.
            Supported keys:
                - keywords : str - Full-text search
                - creator : str - Creator name filter (from extras.creatorName)
                - organization : str - Organization name filter (auto-slugified)
                - license : str - License filter
                - tags : list - List of tags to filter by
                - groups : list - List of groups/collections to filter by (auto-slugified)
                - formats : list - List of resource formats (e.g., ['CSV', 'JSON'])
                - limit : int - Maximum number of datasets to retrieve (default: 100)
        `**kwargs` : dict
            Additional keyword arguments:
                - api_key : str, optional
                    API key for authentication
                - verify_ssl : bool, optional
                    Toggle SSL verification (default False)
        """

        DEFAULT_URL = "https://nationaldataplatform.org/catalog"

        base_url = url or DEFAULT_URL

        # ----------------------------------------------------------------------
        # Auth / Connection Config
        # ----------------------------------------------------------------------
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
        # Tier 1: datasets, Tier 2: per-dataset resource tables
        self._cache = OrderedDict()
        self._dataset_id_map = {}
        self._dataset_title_map = {}

        self._loaded = False
        self.params = params or {}

        # Validate connection before attempting to load data
        try:
            self.validate_connection()
        except (ConnectionError, RuntimeError):
            self._loaded = False
            raise

        # Initial data load (only if connection is valid and params provided)
        if self.params:
            try:
                self._load_initial_data(self.params)
                self._loaded = True
            except Exception as e:
                self._loaded = False
                raise RuntimeError(f"Failed to load initial data: {e}") from e
        else:
            self._loaded = True


    # ----------------------------------------------------------------------
    # Connection Validation
    # ----------------------------------------------------------------------
    def validate_connection(self):
        """
        Validates that the base CKAN URL is accessible and functional.
        
        This method tests the connection by making a simple API call to verify:
            - The URL is reachable
            - The CKAN API is responding
        
        Raises
        ------
        ConnectionError
            If the URL cannot be reached
        RuntimeError
            If the CKAN API returns an error response
        
        Returns
        -------
        bool
            True if connection is valid
        """
        try:
            test_url = f"{self.base_url}/api/3/action/status_show"
            
            response = requests.get(
                test_url,
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=2
            )
            
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise RuntimeError(
                    f"CKAN API at {self.base_url} returned failure response"
                )
            
            return True
        except: # noqa: E722
            # Need to silent exit to continue external workflows
            return False
        
        # except requests.exceptions.Timeout:
        #     raise ConnectionError(
        #         f"Connection timeout: Unable to reach {self.base_url} within 10 seconds"
        #     )
        # except requests.exceptions.ConnectionError:
        #     raise ConnectionError(
        #         f"Connection failed: Unable to connect to {self.base_url}. "
        #         "Check the URL and your network connection."
        #     )
        # except requests.exceptions.HTTPError as e:
        #     if e.response.status_code == 404:
        #         raise RuntimeError(
        #             f"CKAN API not found at {self.base_url}. "
        #             "Verify this is a valid CKAN endpoint."
        #         )
        #     else:
        #         raise RuntimeError(
        #             f"HTTP {e.response.status_code} Error: {str(e)}"
        #         )
        # except requests.exceptions.RequestException as e:
        #     raise ConnectionError(
        #         f"Failed to validate connection to {self.base_url}: {str(e)}"
        #     )
        # except ValueError as e:
        #     raise RuntimeError(
        #         f"Invalid JSON response from {self.base_url}: {str(e)}"
        #     )


    # ----------------------------------------------------------------------
    # Initial Data Load
    # ----------------------------------------------------------------------
    def _load_initial_data(self, params):
        """
        Loads data from NDP API based on query parameters.
        
        Supports:
            - Single query (dict)
            - Multiple queries (list of dicts)
            - Direct ID lookup (id parameter)
        
        Results are deduplicated by dataset ID and stored in a unified structure:
            - Tier 1: datasets table (one row per dataset)
            - Tier 2: resources table (combined resources from ALL datasets)
        
        Parameters
        ----------
        params : dict or list of dict
            Query parameters or list of query parameter dicts.
            Each dict can contain:
                - id : str - Direct dataset ID lookup
                - keywords : str - Full-text search
                - creator : str - Creator name filter (from extras.creatorName)
                - organization : str - Organization filter (auto-slugified)
                - license : str - License filter
                - tags : list - List of tags
                - groups : list - List of groups/collections (auto-slugified)
                - formats : list - List of resource formats (e.g., ['CSV', 'JSON'])
                - limit : int - Maximum number of datasets (default: 100)
        """
        
        # Normalize params to list
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")
        
        # Collect all datasets from all queries
        all_datasets = []
        
        for query_params in query_list:
            # Check if this is a direct ID lookup
            if "id" in query_params:
                dataset = self._get_dataset_by_id(query_params["id"])
                if dataset:
                    all_datasets.append(dataset)
            else:
                # Standard search query
                result = self._run_single_query(query_params)
                all_datasets.extend(result.get("results", []))
        
        # Deduplicate by dataset ID
        unique_datasets = self._deduplicate_datasets(all_datasets)
        
        # Extract tables from deduplicated datasets
        dataset_rows, all_resource_rows, id_map = self._extract_tables(unique_datasets)
        
        # Tier 1: datasets
        self._cache["datasets"] = self._rows_to_table(dataset_rows)
        
        # Tier 2: resources (ONE table for ALL resources)
        if all_resource_rows:
            self._cache["resources"] = self._rows_to_table(all_resource_rows)
        
        self._dataset_id_map = id_map
        self._dataset_title_map = {v: k for k, v in id_map.items()}
       
        self._loaded = True
        
        
    def _slugify(self, value):
        """
        Convert a value to CKAN-compatible slug format.
        
        CKAN stores organization and group names as lowercase slugs
        with spaces replaced by hyphens.
        
        Parameters
        ----------
        value : str
            Value to slugify
            
        Returns
        -------
        str
            Slugified value
        """
        if not isinstance(value, str):
            return str(value).lower()
        
        return value.lower().replace(" ", "-")


    def _quote_if_needed(self, value):
        """
        Quote a value if it contains spaces or special Solr characters.
        
        Parameters
        ----------
        value : str
            Value to potentially quote
            
        Returns
        -------
        str
            Quoted or unquoted value
        """
        if not isinstance(value, str):
            return str(value)
        
        # Quote if contains spaces
        if ' ' in value:
            # Escape any existing quotes within the value
            value = value.replace('"', '\\"')
            return f'"{value}"'
        
        return value


    def _get_dataset_by_id(self, dataset_id):
        """
        Retrieve a single dataset by ID or name using package_show.
        
        Parameters
        ----------
        dataset_id : str
            Dataset ID or name
            
        Returns
        -------
        dict or None
            Dataset dict if found, None otherwise
        """
        try:
            result = self._request("package_show", {"id": dataset_id})
            return result
        except Exception as e:
            print(f"Warning: Could not retrieve dataset '{dataset_id}': {e}")
            return None


    def _run_single_query(self, params):
        """
        Execute a single CKAN query with proper value normalization.
        
        Handles:
            - Slugification for organization and groups (lowercase + hyphens)
            - Quoting for values with spaces (tags, creator, license)
            - Multiple filter combinations
        
        Parameters
        ----------
        params : dict
            Query parameters. Supported keys:
                - keywords : str
                    Full-text search (no normalization)
                - creator : str
                    Creator name filter (quoted if spaces)
                - organization : str
                    Organization name filter (slugified: lowercase, spaces → hyphens)
                - license : str
                    License filter (quoted if spaces)
                - tags : list of str
                    Tag filters (each quoted if spaces)
                - groups : list of str
                    Group/collection filters (each slugified)
                - formats : list of str
                    Resource format filters (each quoted if spaces)
                - limit : int, default 100
                    Maximum number of results
        
        Returns
        -------
        dict
            CKAN API response containing:
                - success : bool
                - result : dict with 'results' list
        """
        query_params = {"rows": params.get("limit", 100)}
        
        q_parts, fq_parts = [], []
        
        # Keywords search
        if params.get("keywords"):
            q_parts.append(params["keywords"])
        
        # Creator filter (searches extras.creatorName)
        if params.get("creator"):
            creator_value = self._quote_if_needed(params["creator"])
            fq_parts.append(f"creatorName:{creator_value}")
        
        # Organization filter (slugify)
        if params.get("organization"):
            org_value = self._slugify(params["organization"])
            fq_parts.append(f"organization:{org_value}")
        
        # License filter (quote if has spaces)
        if params.get("license"):
            license_value = self._quote_if_needed(params["license"])
            fq_parts.append(f"license_id:{license_value}")
        
        # Tags filter (quote each if has spaces)
        if params.get("tags"):
            for tag in params["tags"]:
                tag_value = self._quote_if_needed(tag)
                fq_parts.append(f"tags:{tag_value}")
        
        # Groups filter (slugify each)
        if params.get("groups"):
            for group in params["groups"]:
                group_value = self._slugify(group)
                fq_parts.append(f"groups:{group_value}")
        
        # Format filter (quote each if has spaces)
        if params.get("formats"):
            format_parts = []
            for fmt in params["formats"]:
                fmt_value = self._quote_if_needed(fmt)
                format_parts.append(f"res_format:{fmt_value}")
            fq_parts.append("(" + " OR ".join(format_parts) + ")")
        
        # Build final query params
        if q_parts:
            query_params["q"] = " ".join(q_parts)
        
        if fq_parts:
            query_params["fq"] = " AND ".join(fq_parts)
        
        return self._request("package_search", query_params)


    def _deduplicate_datasets(self, datasets):
        """
        Remove duplicate datasets based on ID.
        
        Parameters
        ----------
        datasets : list
            List of dataset dicts from CKAN API
            
        Returns
        -------
        list
            Deduplicated list of datasets
        """
        seen_ids = set()
        unique_datasets = []
        
        for ds in datasets:
            dataset_id = ds.get("id")
            
            if dataset_id is None:
                # Keep datasets without IDs (shouldn't happen in practice)
                unique_datasets.append(ds)
                continue
            
            if dataset_id not in seen_ids:
                seen_ids.add(dataset_id)
                unique_datasets.append(ds)
        
        return unique_datasets

    
    # ----------------------------------------------------------------------
    # API Helpers
    # ----------------------------------------------------------------------
    def _request(self, endpoint, params=None):
        """
        Execute GET request against CKAN API.

        Parameters
        ----------
        `endpoint` : str
            CKAN API endpoint name
        `params` : dict, optional
            Query parameters for the request

        Returns
        -------
        dict
            Result data from CKAN API response
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


    def _extract_from_extras(self, dataset, key):
        """
        Extract a value from CKAN dataset extras by key.
        
        Parameters
        ----------
        dataset : dict
            CKAN dataset dictionary
        key : str
            The extras key to extract (e.g., 'creatorName')
        
        Returns
        -------
        str or None
            The value if found, None otherwise
        """
        extras = dataset.get("extras", [])
        for extra in extras:
            if extra.get("key") == key:
                return extra.get("value")
        return None
    
    
    def _extract_tables(self, datasets):
        """
        Flatten CKAN dataset JSON into datasets and resources tables.
        
        Creates:
            - datasets: One row per dataset
            - resources: Combined resources from ALL datasets
        
        Parameters
        ----------
        `datasets` : list
            List of dataset dictionaries from CKAN API
        
        Returns
        -------
        tuple
            (dataset_rows, all_resource_rows, id_map) where:
                - dataset_rows : list of dict
                    Flattened dataset metadata
                - all_resource_rows : list of dict
                    Combined resources from all datasets
                - id_map : dict
                    Maps dataset_id to dataset_title
        """
        dataset_rows = []
        all_resource_rows = []
        id_map = {}
        
        for ds in datasets:
            dataset_id = ds.get("id")
            dataset_name = ds.get("name") or dataset_id
            dataset_title = ds.get("title") or dataset_name
            
            if dataset_id and dataset_title:
                id_map[dataset_id] = dataset_title
                
            # Extract creator from extras
            creator_name = self._extract_from_extras(ds, "creatorName")
            creator_email = self._extract_from_extras(ds, "creatorEmail")
            
            dataset_rows.append({
                "id": dataset_id,
                "name": dataset_name,
                "title": dataset_title,
                "notes": ds.get("notes"),
                "organization": (ds.get("organization") or {}).get("title"),
                "creator": creator_name,
                "creator_email": creator_email,
                "group": ",".join(g["name"] for g in ds.get("groups", [])),
                "license": ds.get("license_title"),
                "created": ds.get("metadata_created"),
                "modified": ds.get("metadata_modified"),
                "tags": ",".join(t["name"] for t in ds.get("tags", [])),
                "num_resources": ds.get("num_resources", 0),
                "raw_dataset": ds
            })
            
            for r in ds.get("resources", []):
                all_resource_rows.append({
                    "resource_id": r.get("id"),
                    "resource_name": r.get("name"),
                    "issue_date": r.get("issueDate"),
                    "format": r.get("format"),
                    "size": r.get("size"),
                    "url": r.get("url"),
                    "dataset_id": dataset_id,
                    "dataset_title": dataset_title,
                    "raw_resource": r
                })
        
        return dataset_rows, all_resource_rows, id_map


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

        cols = list(rows[0].keys())
        table = OrderedDict({c: [] for c in cols})

        for r in rows:
            for c in cols:
                table[c].append(r.get(c))

        return table


    # ----------------------------------------------------------------------
    # Terminal Methods
    # ----------------------------------------------------------------------
    def num_datasets(self):
        """
        Returns the number of datasets (rows in the datasets table).
        
        Returns
        -------
        int
            Number of datasets loaded
        """
        if not self._loaded:
            return 0
        
        datasets_table = self._cache.get("datasets", {})
        
        if not datasets_table:
            return 0
        
        # Get length of first column to determine row count
        first_col = next(iter(datasets_table.values()), [])
        return len(first_col)


    def num_tables(self):
        """
        Returns the number of tables currently loaded.
        
        NDP backend has 2 main tables:
            - datasets: Dataset metadata
            - resources: Combined resources from all datasets
        
        Returns
        -------
        None
            Prints the count to console
        """
        if not self._loaded:
            print("0 tables loaded")
            return
        
        # Count actual tables in cache
        table_count = 0
        
        # Check for datasets table
        if "datasets" in self._cache and self._cache["datasets"]:
            table_count += 1
        
        # Check for resources table
        if "resources" in self._cache and self._cache["resources"]:
            table_count += 1
        
        # Check for errors table (if implemented)
        if "errors" in self._cache and self._cache["errors"]:
            table_count += 1
        
        print(f"{table_count} tables loaded")


    def get_table(self, table_name, dict_return=False):
        """
        Returns all data from a specified table.
        
        Parameters
        ----------
        `table_name` : str
            Must be 'datasets' or 'resources'
        `dict_return` : bool, default False
            If True, returns OrderedDict. If False, returns DataFrame.
        
        Returns
        -------
        OrderedDict or pandas.DataFrame
        """
        if not self._loaded:
            raise RuntimeError("No data loaded")
        
        if table_name not in self._cache:
            raise ValueError(
                f"Table '{table_name}' not found. "
                f"Available tables: {list(self._cache.keys())}"
            )
        
        table = self._cache.get(table_name)
        
        if not table:
            raise ValueError(f"Table '{table_name}' is empty")
        
        if dict_return:
            return table
        return pd.DataFrame(table)

    
    def get_schema(self):
        """NDP does not store structural schema - data comes from CKAN API."""
        return (
            "-- NDP Backend Schema Information\n"
            "-- NDP is a read-only CKAN metadata backend\n"
            "-- Data is retrieved dynamically from the API\n"
            "-- Use summary() or list() to view available tables and columns\n"
        )


    def get_table_names(self, query):
        """
        Extracts table/dataset names mentioned in a query string.
        
        Parameters
        ----------
        `query` : str
            Query string to parse
        
        Returns
        -------
        list
            List of dataset names/IDs found in query
        """
        if not self._loaded:
            return []
        
        import re
        
        pattern = r'\b[a-zA-Z_][a-zA-Z0-9_-]*\b'
        words = re.findall(pattern, query)
        
        found_tables = []
        for word in words:
            if word in self._cache:
                found_tables.append(word)
            elif word in self._dataset_id_map:
                found_tables.append(self._dataset_id_map[word])
        
        return list(set(found_tables))


    def overwrite_table(self, table_name, collection):
        """
        Not supported - NDP backend is read-only.
        
        Parameters
        ----------
        `table_name` : str or list
            Table name(s)
        `collection` : DataFrame or list
            Data
        
        Raises
        ------
        NotImplementedError
            Always raised as NDP is read-only
        """
        raise NotImplementedError(
            "NDP backend is read-only. Cannot overwrite tables. "
            "To modify data, use artifact_handler('process') to load into "
            "a writable backend (Sqlite/DuckDB), make changes, then query."
        )


    # ----------------------------------------------------------------------
    # Query Interface (in-memory)
    # ----------------------------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query not supported for NDP backend (non-SQL backend).
        
        NDP is a read-only metadata backend that does not support SQL queries.
        Use find() or find_relation() for searching data instead.

        Parameters
        ----------
        `query` : str
            Query string (unused)
        `dict_return` : bool, default True
            Return format flag (unused)
        `**kwargs` : dict
            Additional keyword arguments (unused)

        Raises
        ------
        NotImplementedError
            Always raised as NDP does not support SQL queries
        """
        raise NotImplementedError(
            "query() is not supported for NDP backend - it is a non-SQL backend.\n"
            "Please use find() or find_relation() instead for searching data.\n\n"
            "Examples:\n"
            "  # Search for a value across all tables\n"
            "  dsi.find('num_resources > 5')\n\n"
            "  # Find rows matching a condition\n"
            "  dsi.find('organization == \"Oceans11 - LANL\"')\n"
        )
    
    # def find_relation(self, query, dict_return=True, **kwargs):
    #     """
    #     Query all tables using a pandas query string.

    #     Parameters
    #     ----------
    #     `query` : str
    #         Pandas query string for filtering data
    #     `dict_return` : bool, default True
    #         If True, returns dict format.
    #         If False, returns pandas DataFrames.
    #     `**kwargs` : dict
    #         Additional keyword arguments

    #     Returns
    #     -------
    #     dict
    #         Dictionary mapping table names to query results
    #     """

    #     if not self._loaded:
    #         raise RuntimeError("No data loaded. Cannot query empty backend.")

    #     results = {}

    #     for t_name, table in self._cache.items():
    #         df = pd.DataFrame(table)

    #         if df.empty:
    #             continue

    #         try:
    #             result_df = df.query(query, engine="python")

    #             if not result_df.empty:
    #                 results[t_name] = (
    #                     result_df.to_dict(orient="list")
    #                     if dict_return else result_df
    #                 )
    #         except pd.errors.UndefinedVariableError:
    #             continue
    #         except Exception as e:
    #             raise ValueError(f"Query error in {t_name}: {e}")

    #     if not results:
    #         raise ValueError(f"Query returned no results: '{query}'")

    #     return results

    def find_relation(self, column_name, relation, **kwargs):
        """
        Finds all rows across all tables where a column relation is satisfied.
        
        DSI core calls this with parsed query components.
        
        Parameters
        ----------
        column_name : str
            The column to query (e.g., "num_resources")
        relation : str
            The relation/operator (e.g., "< 2", "> 5", "== 'value'")
        **kwargs : dict
            Additional keyword arguments
        
        Returns
        -------
        list of ValueObject
            List of ValueObjects representing matching rows
        """
        
        if not self._loaded:
            return []
        
        # Reconstruct pandas query from column_name and relation
        query = f"{column_name} {relation}"
        
        matches = []
        
        for table_name, table in self._cache.items():
            df = pd.DataFrame(table)
            
            if df.empty:
                continue
            
            # Check if column exists in this table
            if column_name not in df.columns:
                continue
            
            try:
                # Reset index to ensure 0-based indexing
                df = df.reset_index(drop=True)
                
                # Use pandas query to filter rows
                result_df = df.query(query, engine="python")
                
                if result_df.empty:
                    continue
                
                # Convert matching rows to ValueObjects
                for idx, row in result_df.iterrows():
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = list(df.columns)
                    val.row_num = idx + 1  # 1-indexed row numbers
                    val.value = row.tolist()
                    val.type = "cell"
                    
                    matches.append(val)
                    
            except pd.errors.UndefinedVariableError:
                # Column doesn't exist in this table - skip it
                continue
            except Exception:
                # Query failed for this table - skip it
                continue
        
        return matches


    # ----------------------------------------------------------------------
    # Artifact Processing (tiered table construction)
    # ----------------------------------------------------------------------
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

        Returns
        -------
        OrderedDict
            All cached tables in tiered structure
        """

        if not self._loaded:
            return {}

        return self._cache


    def validate_urls(self):
        """
        Validates resource URLs in the unified resources table.
        Adds 'url_valid' column to resources table.
        """
        # Only validate the unified resources table
        if "resources" not in self._cache:
            print("No resources table found")
            return
        
        table = self._cache.get("resources", {})
        urls = table.get("url", [])
        
        if not urls:
            print("No URLs found in resources table")
            return
        
        headers = {"User-Agent": "NDP-Validator"}
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


    # # ----------------------------------------------------------------------
    # # URL Validation
    # # ----------------------------------------------------------------------
    # def validate_urls(self):
    #     """
    #     Validates resource URLs across all resource tables.
    #     Adds 'url_valid' column to each resource table.
    #     """
    #     headers = {"User-Agent": "NDP-Validator"}

    #     for table_name in self._resource_tables:
    #         table = self._cache.get(table_name, {})
    #         urls = table.get("url", [])

    #         valid_list = []

    #         for url in urls:
    #             try:
    #                 r = requests.head(
    #                     url,
    #                     allow_redirects=True,
    #                     headers=headers,
    #                     timeout=10,
    #                     verify=self.verify_ssl
    #                 )

    #                 if r.status_code == 405:
    #                     r = requests.get(
    #                         url,
    #                         stream=True,
    #                         headers=headers,
    #                         timeout=10,
    #                         verify=self.verify_ssl
    #                     )

    #                 valid_list.append(200 <= r.status_code < 400)

    #             except Exception:
    #                 valid_list.append(False)

    #         table["url_valid"] = valid_list


    # ----------------------------------------------------------------------
    # Find Methods
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """
        Searches for all instances of query_object across all tables.

        Searches at the table, column, and cell levels.

        Parameters
        ----------
        `query_object` : int, float, or str
            The value to search for across all tables in the backend
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        list of ValueObject
            A list of ValueObjects representing matches across:
                - table names
                - column names
                - cell values

        Notes
        -----
        ValueObject Structure:
            - t_name : str
                Table name
            - c_name : list
                Column name(s)
            - row_num : int or None
                Row index
            - value : any
                Matched value or data
            - type : str
                {'table', 'column', 'cell'}
        """
        
        query_str = str(query_object).lower()

        return (
            self.find_table(query_str) +
            self.find_column(query_str) +
            self.find_cell(query_object)
        )


    def find_table(self, query_object, **kwargs):
        """
        Finds all tables whose names contain the given query_object.

        Search is case-insensitive.

        Parameters
        ----------
        `query_object` : str
            The string to match against table names
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        list of ValueObject
            One ValueObject per matching table

        Notes
        -----
        ValueObject Structure:
            - t_name : str
                Table name
            - c_name : list
                List of all columns in the table
            - value : dict
                Full table data (dict of columns)
            - row_num : None
            - type : 'table'
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
        Finds all columns whose names contain the given query_object.

        Search is case-insensitive.

        Parameters
        ----------
        `query_object` : str
            The string to match against column names
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        list of ValueObject
            One ValueObject per matching column

        Notes
        -----
        ValueObject Structure:
            - t_name : str
                Table name
            - c_name : list
                List containing the matched column name
            - value : list
                Full column data
            - row_num : None
            - type : 'column'
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
        Finds all cells that match the given query_object.

        Matching behavior:
            - Exact match for all data types
            - Case-insensitive partial match for strings

        Parameters
        ----------
        `query_object` : int, float, or str
            The value to search for within table cells
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        list of ValueObject
            One ValueObject per matching cell

        Notes
        -----
        ValueObject Structure:
            - t_name : str
                Table name
            - c_name : list
                List containing the matched column name
            - row_num : int
                Row index of the match
            - value : any
                Matched cell value
            - type : 'cell'
        """

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


    # def find_relation(self, column_name, relation, **kwargs):
    #     """
    #     Not supported for NDP backend.

    #     NDP is a read-only metadata backend and does not support
    #     relational queries on columns.

    #     Parameters
    #     ----------
    #     `column_name` : str
    #         Column name (unused)
    #     `relation` : str
    #         Relation type (unused)
    #     `**kwargs` : dict
    #         Additional keyword arguments (unused)

    #     Returns
    #     -------
    #     list
    #         Always returns an empty list
    #     """
    #     return []


    # ----------------------------------------------------------------------
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        Lists tables or prints metadata in SQLite-compatible format.
        
        Parameters
        ----------
        `collection` : bool, default False
            If True, return list of table names.
            If False, print table names with dimensions.
        
        Returns
        -------
        list or None
            Table names if collection=True, otherwise None
        """
        if collection:
            return list(self._cache.keys())
        
        # Print in SQLite-compatible format
        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"Table: {name}")
            print(f"  - num of columns: {len(df.columns)}")
            print(f"  - num of rows: {len(df)}")
            print()
    
    
    def summary(self, table_name=None, collection=False):
        """
        Returns detailed column-level statistics for tables.
        
        Parameters
        ----------
        table_name : str, optional
            If provided, returns summary for that table.
            Must be 'datasets', 'resources', or 'tags'.
        collection : bool, default False
            If True, returns data as DataFrame(s).
            If False, returns list format for Terminal to print.
        
        Returns
        -------
        pandas.DataFrame or list
            - If collection=True and table_name specified: returns single DataFrame
            - If collection=True and table_name=None: returns list [table_names, df1, df2, ...]
            - If collection=False: returns list [table_names, df1, df2, ...] for Terminal
        
        Notes
        -----
        - For OBJECT columns: min/max are lexicographic (alphabetical) for short text
        - Skips min/max for: long text (>80 chars), URLs, metadata, complex values
        - Numeric columns get full statistics: min, max, avg, std_dev
        """
        def is_complex_value(value):
            """Check if value is a complex type (dict, list, etc.)"""
            return isinstance(value, (dict, list, tuple, set))
        
        def is_url_or_metadata_column(column):
            """Check if column contains URLs or metadata that shouldn't have min/max"""
            return column.lower() in {
                "raw_metadata", "landing_page", "metadata_url", 
                "download_url", "url", "href", "link", "raw_dataset"
            }
        
        def is_long_text_series(series):
            """Check if series contains long text (> 80 chars)"""
            non_null = series.dropna()
            if non_null.empty:
                return False
            string_series = non_null.astype(str)
            return string_series.str.len().max() > 80
        
        def safe_to_python(value):
            """Convert pandas/numpy types to Python native types, NaN to None"""
            if pd.isna(value):
                return None
            if isinstance(value, (np.integer, np.floating)):
                return value.item()
            return value
        
        def summarize_dataframe(df):
            """Generate column-level statistics for a DataFrame"""
            rows = []
            
            for column in df.columns:
                original_series = df[column]
                non_null = original_series.dropna()
                
                # Check if column has complex values
                has_complex_values = (
                    non_null.apply(is_complex_value).any()
                    if not non_null.empty
                    else False
                )
                
                # Convert complex values to strings for counting
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
                
                # Try numeric stats first
                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
                
                if not non_null.empty and len(numeric_series) == len(non_null):
                    # Fully numeric column
                    row["min"] = safe_to_python(numeric_series.min())
                    row["max"] = safe_to_python(numeric_series.max())
                    row["avg"] = safe_to_python(numeric_series.mean())
                    row["std_dev"] = safe_to_python(numeric_series.std())
                
                elif (
                    not non_null.empty
                    and not has_complex_values
                    and not is_url_or_metadata_column(column)
                    and not is_long_text_series(non_null)
                ):
                    # Short text column - show min/max (lexicographic/alphabetical)
                    try:
                        row["min"] = safe_to_python(non_null.min())
                        row["max"] = safe_to_python(non_null.max())
                    except TypeError:
                        # In case min/max fail for some reason
                        row["min"] = None
                        row["max"] = None
                
                rows.append(row)
            
            summary_df = pd.DataFrame(
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
            
            # Replace NaN with None for cleaner output
            summary_df = summary_df.replace({np.nan: None})
            
            return summary_df
        
        # Check if backend is loaded
        if not self._loaded:
            if collection:
                return pd.DataFrame()
            else:
                return [[], pd.DataFrame()]
        
        # Single table summary
        if table_name:
            if table_name not in self._cache:
                raise ValueError(
                    f"Table '{table_name}' not found. "
                    f"Available tables: {list(self._cache.keys())}"
                )
            
            table = self._cache.get(table_name)
            
            if not table:
                raise ValueError(f"Table '{table_name}' is empty")
            
            df = pd.DataFrame(table)
            summary_df = summarize_dataframe(df)
            
            if collection:
                return summary_df
            else:
                return [[table_name], summary_df]
        
        # Multiple tables summary
        table_names = []
        summary_dfs = []
        
        for name, table in self._cache.items():
            if not table:
                continue
                
            df = pd.DataFrame(table)
            summary_df = summarize_dataframe(df)
            
            table_names.append(name)
            summary_dfs.append(summary_df)
        
        return [table_names] + summary_dfs
    

    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Displays rows from a specified table.

        By default, shows a curated subset of columns for readability.
        Raw metadata columns (raw_*) are always excluded unless explicitly requested.

        Parameters
        ----------
        `table_name` : str
            Name of the table to display
        `num_rows` : int, default 25
            Number of rows to display
        `display_cols` : list of str or 'all', optional
            Subset of columns to display. Options:
            - None: Shows smart default columns for the table
            - 'all': Shows all columns except raw_*
            - [...]: Shows specific columns you list

        Returns
        -------
        None
            Prints formatted table to console
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")
        
        # Validate table_name type
        if not isinstance(table_name, str):
            raise TypeError("display() ERROR: Input 'table_name' must be a string")
        
        # Validate display_cols type - allow None, list, or 'all'
        if display_cols is not None and display_cols != 'all' and not isinstance(display_cols, list):
            raise TypeError("display() ERROR: Input 'display_cols' must be a list of column names or 'all'")
        
        # If display_cols is a list, validate it contains strings
        if isinstance(display_cols, list):
            if not all(isinstance(col, str) for col in display_cols):
                raise TypeError("display() ERROR: All elements in 'display_cols' must be strings")
        
        # Validate num_rows type
        if num_rows is not None and not isinstance(num_rows, int):
            raise TypeError("display() ERROR: Input 'num_rows' must be an integer or None")
        
        if num_rows is not None and num_rows <= 0:
            raise ValueError("display() ERROR: Input 'num_rows' must be positive")
        
        # Direct lookup - no resolution needed
        if table_name not in self._cache:
            raise ValueError(
                f"Table '{table_name}' not found. "
                f"Available tables: {list(self._cache.keys())}"
            )

        table = self._cache.get(table_name)

        if not table:
            raise ValueError(f"Table '{table_name}' is empty")

        df = pd.DataFrame(table)

        # Define default columns for each table
        default_display_cols = {
            'datasets': ['id', 'name', 'title', 'organization', 'num_resources'],
            'resources': ['resource_id', 'resource_name', 'format', 'url', 'dataset_title']
        }

        # Determine which columns to display
        if display_cols == 'all':
            # Show all non-raw columns
            cols_to_show = [col for col in df.columns if not col.startswith('raw_')]
        elif display_cols:
            # User specified exact columns
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in '{table_name}': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            cols_to_show = display_cols
        else:
            # Use smart defaults for this table
            default_cols = default_display_cols.get(table_name, [])
            
            # Filter to only columns that exist in the dataframe
            cols_to_show = [col for col in default_cols if col in df.columns]
            
            # Fallback: if no defaults defined or none exist, show non-raw columns
            if not cols_to_show:
                cols_to_show = [col for col in df.columns if not col.startswith('raw_')]
            
            # Ultimate fallback: show first column at minimum
            if not cols_to_show:
                cols_to_show = df.columns[:1].tolist()

        df = df[cols_to_show]

        # Limit rows
        total_rows = len(df)
        display_df = df.head(num_rows) if num_rows else df

        # Truncate long values for display
        def truncate_value(x):
            if isinstance(x, (dict, list)):
                x = str(x)
            if isinstance(x, str) and len(x) > 50:
                return x[:50] + '...'
            return x
        
        display_df = display_df.map(truncate_value)

        # Print header
        print(f"\nTable: {table_name}\n")
        
        # Format and print table
        headers = display_df.columns.tolist()
        rows = display_df.values.tolist()
        
        # Calculate column widths
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                val_str = str(val) if val is not None else 'None'
                col_widths[i] = max(col_widths[i], len(val_str))
        
        # Print header row
        header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
        print(header_line)
        print("-" * len(header_line))
        
        # Print data rows
        for row in rows:
            row_strs = [str(val) if val is not None else 'None' for val in row]
            print(" | ".join(s.ljust(w) for s, w in zip(row_strs, col_widths)))
        
        # Print row count info
        if num_rows and total_rows > num_rows:
            print(f"\n... showing {num_rows} of {total_rows} rows")
        
        print()


    def notebook(self, **kwargs):
        """
        Notebook generation not supported for NDP backend.

        Parameters
        ----------
        `**kwargs` : dict
            Additional keyword arguments (unused)

        Returns
        -------
        None
        """
        pass


    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    def close(self):
        """
        Resets backend state and clears all cached data.
        """

        self._cache = OrderedDict()
        self._dataset_id_map = {}
        self._dataset_title_map = {}
        self._loaded = False


    # ----------------------------------------------------------------------
    # Abstract Methods
    # ----------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest not supported for NDP backend (read-only).

        Parameters
        ----------
        `artifacts` : any
            Artifacts to ingest (unused)
        `**kwargs` : dict
            Additional keyword arguments (unused)

        Returns
        -------
        None

        Raises
        ------
        NotImplementedError
            Always raised as NDP backend is read-only
        """
        raise NotImplementedError("NDP backend is read-only")
    