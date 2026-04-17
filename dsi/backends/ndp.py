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
                - keywords : str - Search keywords
                - organization : str - Organization name filter
                - tags : list - List of tags to filter by
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
        # Auth / connection config
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
        self._resource_tables = []
        self._dataset_id_map = {}
        self._dataset_title_map = {}

        self._loaded = False
        self.params = params or {}

        # Validate connection FIRST before attempting to load data
        try:
            self.validate_connection()
        except (ConnectionError, RuntimeError):
            self._loaded = False
            raise

        # Initial data load (only if connection is valid and params provided)
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
                timeout=10
            )
            
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                raise RuntimeError(
                    f"CKAN API at {self.base_url} returned failure response"
                )
            
            return True
            
        except requests.exceptions.Timeout:
            raise ConnectionError(
                f"Connection timeout: Unable to reach {self.base_url} within 10 seconds"
            )
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Connection failed: Unable to connect to {self.base_url}. "
                "Check the URL and your network connection."
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise RuntimeError(
                    f"CKAN API not found at {self.base_url}. "
                    "Verify this is a valid CKAN endpoint."
                )
            else:
                raise RuntimeError(
                    f"HTTP {e.response.status_code} Error: {str(e)}"
                )
        except requests.exceptions.RequestException as e:
            raise ConnectionError(
                f"Failed to validate connection to {self.base_url}: {str(e)}"
            )
        except ValueError as e:
            raise RuntimeError(
                f"Invalid JSON response from {self.base_url}: {str(e)}"
            )


    # ----------------------------------------------------------------------
    # Initial Data Load
    # ----------------------------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch datasets/resources from CKAN API and store in memory.

        Parameters
        ----------
        `params` : dict
            Query parameters including:
                - keywords : str, optional
                - organization : str, optional
                - tags : list, optional
                - formats : list, optional
                - limit : int, optional
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

        dataset_rows, resource_map, id_map = self._extract_tables(result.get("results", []))

        # Tier 1: datasets
        self._cache["datasets"] = self._rows_to_table(dataset_rows)
        
        self._dataset_id_map = id_map
        self._dataset_title_map = {v: k for k, v in id_map.items()}

        # Tier 2: per-dataset resource tables
        self._resource_tables = []
        for dataset_title, rows in resource_map.items():
            table_name = dataset_title
            self._cache[table_name] = self._rows_to_table(rows)
            self._resource_tables.append(table_name)

        self._loaded = True


    # ----------------------------------------------------------------------
    # Table Name Resolution
    # ----------------------------------------------------------------------
    def _resolve_table_name(self, identifier):
        """
        Resolves a table identifier to its canonical name.

        Accepts either dataset_title or dataset_id for resource tables.

        Parameters
        ----------
        `identifier` : str
            Table name (dataset_title) or dataset ID

        Returns
        -------
        str
            Canonical table name (dataset_title)
            
        Raises
        ------
        ValueError
            If the identifier cannot be resolved to a table
        """
        if identifier in self._cache:
            return identifier

        if identifier in self._dataset_id_map:
            return self._dataset_id_map[identifier]

        available_titles = [name for name in self._resource_tables]
        
        error_msg = f"Table '{identifier}' not found.\n"
        
        if available_titles:
            error_msg += "\nAvailable dataset titles:\n"
            for title in available_titles[:5]:
                dataset_id = self._dataset_title_map.get(title, "N/A")
                error_msg += f"  - '{title}' (ID: {dataset_id})\n"
            if len(available_titles) > 5:
                error_msg += f"  ... and {len(available_titles) - 5} more\n"
        
        if "datasets" in self._cache:
            error_msg += "\nOr use 'datasets' to view all datasets."
        
        raise ValueError(error_msg)
    
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


    def _extract_tables(self, datasets):
        """
        Flatten CKAN dataset JSON into dataset and resource tables.

        Parameters
        ----------
        `datasets` : list
            List of dataset dictionaries from CKAN API

        Returns
        -------
        tuple
            (dataset_rows, resource_map, id_map) where:
                - dataset_rows : list of dict
                    Flattened dataset metadata
                - resource_map : dict
                    Maps dataset_name to list of resource rows
                - id_map : dict
                    Maps dataset_id to dataset_title
        """

        dataset_rows = []
        resource_map = {}
        id_map = {}

        for ds in datasets:
            dataset_id = ds.get("id")
            dataset_name = ds.get("name") or dataset_id
            dataset_title = ds.get("title") or dataset_name
            
            if dataset_id and dataset_title:
                id_map[dataset_id] = dataset_title

            dataset_rows.append({
                "id": dataset_id,
                "name": dataset_name,
                "title": dataset_title,
                "notes": ds.get("notes"),
                "organization": (ds.get("organization") or {}).get("title"),
                "author": ds.get("author"),
                "license": ds.get("license_title"),
                "created": ds.get("metadata_created"),
                "modified": ds.get("metadata_modified"),
                "tags": ",".join(t["name"] for t in ds.get("tags", [])),
                "num_resources": ds.get("num_resources", 0)
            })

            resource_map.setdefault(dataset_title, [])
            
            for r in ds.get("resources", []):
                resource_map[dataset_title].append({
                    "resource_id": r.get("id"),
                    "resource_name": r.get("name"),
                    "format": r.get("format"),
                    "size": r.get("size"),
                    "url": r.get("url"),
                    "dataset_id": dataset_id,
                    "dataset_title": dataset_title
                })

        return dataset_rows, resource_map, id_map


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
    def num_tables(self):
        """
        Prints the number of tables (datasets) loaded.
        """
        if not self._loaded:
            print("0 tables loaded")
            return
        
        num = len(self._cache) - (1 if "datasets" in self._cache else 0)
        print(f"{num} tables loaded")


    def get_table(self, table_name, dict_return=False):
        """
        Returns all data from a specified table.
        
        Parameters
        ----------
        `table_name` : str
            Dataset title or ID
        `dict_return` : bool, default False
            If True, returns OrderedDict. If False, returns DataFrame.
        
        Returns
        -------
        OrderedDict or pandas.DataFrame
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Call load_datasets() first.")
        
        resolved_name = self._resolve_table_name(table_name)
        table = self._cache.get(resolved_name)
        
        if not table:
            raise ValueError(f"Table '{resolved_name}' is empty")
        
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
        Query all tables using a pandas query string.

        Parameters
        ----------
        `query` : str
            Pandas query string for filtering data
        `dict_return` : bool, default True
            If True, returns dict format.
            If False, returns pandas DataFrames.
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        dict
            Dictionary mapping table names to query results
        """

        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot query empty backend.")

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
                continue
            except Exception as e:
                raise ValueError(f"Query error in {t_name}: {e}")

        if not results:
            raise ValueError(f"Query returned no results: '{query}'")

        return results


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


    # ----------------------------------------------------------------------
    # URL Validation
    # ----------------------------------------------------------------------
    def validate_urls(self):
        """
        Validates resource URLs across all resource tables.
        Adds 'url_valid' column to each resource table.
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


    def find_relation(self, column_name, relation, **kwargs):
        """
        Not supported for NDP backend.

        NDP is a read-only metadata backend and does not support
        relational queries on columns.

        Parameters
        ----------
        `column_name` : str
            Column name (unused)
        `relation` : str
            Relation type (unused)
        `**kwargs` : dict
            Additional keyword arguments (unused)

        Returns
        -------
        list
            Always returns an empty list
        """
        return []


    # ----------------------------------------------------------------------
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        Lists tables or prints metadata.

        For resource tables, displays both dataset_title and dataset_id.

        Parameters
        ----------
        `collection` : bool, default False
            If True, return list of table names.
            If False, print table names with dimensions and dataset IDs.

        Returns
        -------
        dict_keys or None
            Table names if collection=True, otherwise None
        """

        if collection:
            return self._cache.keys()

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            
            if name in self._resource_tables:
                dataset_id = self._dataset_title_map.get(name, "N/A")
                print(f"{name} (ID: {dataset_id}): ({len(df)} rows, {len(df.columns)} cols)")
            else:
                print(f"{name}: ({len(df)} rows, {len(df.columns)} cols)")


    def summary(self, table_name=None):
        """
        Returns numerical metadata for tables.

        For resource tables, includes dataset_id information.

        Parameters
        ----------
        `table_name` : str, optional
            If provided, returns summary for a single table.
            Can be either dataset_title or dataset_id.
            If None, returns summary for all tables in expected format.

        Returns
        -------
        pandas.DataFrame or list
            - If table_name is None: returns [table_names_list, df1, df2, ...]
            - If table_name provided: returns single DataFrame
        """

        if not self._loaded:
            return pd.DataFrame()

        if table_name:
            # Single table - return DataFrame
            resolved_name = self._resolve_table_name(table_name)
            table = self._cache.get(resolved_name)
            
            if not table:
                raise ValueError(f"Table '{resolved_name}' is empty")
            
            df = pd.DataFrame(table)
            
            summary_dict = {
                "table_name": resolved_name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns)
            }
            
            if resolved_name in self._resource_tables:
                summary_dict["dataset_id"] = self._dataset_title_map.get(resolved_name, "N/A")
            
            return pd.DataFrame([summary_dict])
        
        # Multiple tables - return list format [table_names, df1, df2, ...]
        table_names = []
        summary_dfs = []
        
        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            
            summary_dict = {
                "table_name": name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns)
            }
            
            if name in self._resource_tables:
                summary_dict["dataset_id"] = self._dataset_title_map.get(name, "N/A")
            
            table_names.append(name)
            summary_dfs.append(pd.DataFrame([summary_dict]))
        
        # Return as [table_names_list, df1, df2, ...]
        return [table_names] + summary_dfs


    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Displays rows from a specified table.

        Accepts either dataset_title or dataset_id for resource tables.

        Parameters
        ----------
        `table_name` : str
            Title or ID of the table to display
        `num_rows` : int, default 25
            Number of rows to display
        `display_cols` : list of str, optional
            Subset of columns to display

        Returns
        -------
        pandas.DataFrame
            Displayed table data with long strings truncated
        """

        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        resolved_name = self._resolve_table_name(table_name)

        table = self._cache.get(resolved_name)

        if not table:
            raise ValueError(f"Table '{resolved_name}' is empty")

        df = pd.DataFrame(table)

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in '{resolved_name}': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        # Set max_rows before limiting rows
        df.attrs["max_rows"] = len(df)
        
        if num_rows:
            df = df.head(num_rows)

        # Truncate long strings for display using df.map() (pandas 2.1.0+)
        df = df.map(
            lambda x: (str(x)[:60] + '...') if isinstance(x, str) and len(str(x)) > 60 else x
        )

        return df

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
        self._resource_tables = []
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
    