"""
Maglab-OSF Backend for DSI

Read-only backend that pulls metadata from the OSF (Open Science Framework)
REST API for MagLab-published datasets and exposes it as in-memory DSI
tables: datasets, files and relationships.
"""

from collections import OrderedDict
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
import urllib3

from dsi.backends.webserver import Webserver

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
# Maglab Backend (Webserver - Read only)
# ----------------------------------------------------------------------
class Maglab(Webserver):
    """
    OSF-based web backend for querying MagLab dataset metadata in-memory.
    """
    read_only = True

    DEFAULT_URL = "https://api.osf.io/v2"

    # ----------------------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from the OSF API.

        Parameters
        ----------
        `url` : str, optional
            Base OSF API URL. If None, the public OSF API is used.
        `params` : dict or list of dict, optional
            Dictionary of initial query parameters used to fetch data from OSF.

            Supported keys:
                - node_id : str, required - OSF node id (e.g. "8r2b3")
                - provider : str, optional - Storage provider (default "osfstorage")
                - include_ext : str or list of str, optional - Only include files
                  with these extensions (e.g. ".tdms")
                - exclude_ext : str or list of str, optional - Exclude files with
                  these extensions

            A list of dicts fetches and merges multiple OSF nodes into one
            `datasets` table and one `files` table.
        `**kwargs` : dict
            Additional keyword arguments:
                - api_key : str, optional
                    OSF personal access token for authenticated requests
                - verify_ssl : bool, optional
                    Toggle SSL verification (default False)
        """

        base_url = url or self.DEFAULT_URL

        # ----------------------------------------------------------------------
        # Auth / Connection Config
        # ----------------------------------------------------------------------
        self.api_key = kwargs.get("api_key")
        self.verify_ssl = kwargs.get("verify_ssl", False)

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")

        self.headers = {"Accept": "application/vnd.api+json"}
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

        # skip data retrieval if only checking connection
        if kwargs.get("only_validate", False):
            return

        self._cache = OrderedDict()

        self._loaded = False
        self.params = params or {}
        self.validate_error_msg = None

        # Validate connection before attempting to load data
        if not self.validate_connection():
            self._loaded = False
            raise ConnectionError(self.validate_error_msg or f"Unable to connect to OSF API at {self.base_url}")

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
        Validates that the base OSF API URL is accessible and functional.

        This method tests the connection by making a simple API call to verify:
            - The URL is reachable
            - The OSF API is responding

        Returns
        -------
        bool
            True if connection is valid.
            False if connection is not valid.
        """
        try:
            test_url = f"{self.base_url}/nodes/"

            response = requests.get(
                test_url,
                params={"page[size]": 1},
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=5,
            )
            response.raise_for_status()
            data = response.json()

            if "data" not in data:
                self.validate_error_msg = f"OSF API at {self.base_url} returned an unexpected response"
                return False

            return True

        except requests.exceptions.Timeout:
            self.validate_error_msg = f"Connection timeout: Cannot reach {self.base_url}"
            return False
        except requests.exceptions.ConnectionError:
            self.validate_error_msg = f"Connection failed: Cannot connect to {self.base_url}"
            return False
        except requests.exceptions.RequestException as e:
            self.validate_error_msg = f"Failed to validate connection to {self.base_url}: {e}"
            return False
        except ValueError as e:
            self.validate_error_msg = f"Invalid JSON response from {self.base_url}: {e}"
            return False
        except Exception:
            return False

    # ----------------------------------------------------------------------
    # Initial Data Load
    # ----------------------------------------------------------------------
    def _load_initial_data(self, params):
        """
        Loads data from the OSF API based on query parameters.

        Supports:
            - Single query (dict)
            - Multiple queries (list of dicts)

        Results are deduplicated and stored as:
            - datasets : one row per OSF node
            - files : combined file metadata from ALL requested nodes
            - relationships : one row per relationship pointer on each
              requested node (e.g. children, parent, contributors), excluding
              'files' since that relationship is already expanded above

        Parameters
        ----------
        params : dict or list of dict
            Query parameters or list of query parameter dicts.
            Each dict can contain:
                - node_id : str, required - OSF node id (e.g. "8r2b3")
                - provider : str, optional - Storage provider (default "osfstorage")
                - include_ext : str or list of str, optional - Only include files
                  with these extensions (e.g. ".tdms")
                - exclude_ext : str or list of str, optional - Exclude files with
                  these extensions
        """
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")

        all_dataset_rows = []
        all_file_rows = []
        all_relationship_rows = []

        for query_params in query_list:
            node_id = query_params.get("node_id")
            if not node_id:
                raise ValueError("Each params dict must include a 'node_id' key")

            provider = query_params.get("provider", "osfstorage")
            include_ext = self._normalize_ext(query_params.get("include_ext"))
            exclude_ext = self._normalize_ext(query_params.get("exclude_ext"))

            node_data = self._request(f"nodes/{node_id}/")

            all_dataset_rows.append(self._fetch_dataset(node_id, node_data))
            all_file_rows.extend(self._fetch_files(node_id, provider, include_ext, exclude_ext))
            all_relationship_rows.extend(self._fetch_relationships(node_id, node_data))

        unique_datasets = self._deduplicate_rows(all_dataset_rows, "node_id")
        unique_files = self._deduplicate_rows(all_file_rows, "osf_file_id")

        self._cache["datasets"] = self._rows_to_table(unique_datasets)

        if unique_files:
            self._cache["files"] = self._rows_to_table(unique_files)

        if all_relationship_rows:
            self._cache["relationships"] = self._rows_to_table(all_relationship_rows)

        self._loaded = True

    def _normalize_ext(self, exts):
        """
        Normalize an extension filter into a list of lowercase, dot-prefixed strings.

        Parameters
        ----------
        exts : str or list of str
            Extension(s) to normalize (e.g. "tdms" or [".csv", "TXT"])

        Returns
        -------
        list of str or None
            Normalized extensions, or None if no extensions were given
        """
        if not exts:
            return None
        if isinstance(exts, str):
            exts = [exts]
        return [e.lower() if e.startswith(".") else f".{e.lower()}" for e in exts]

    def _deduplicate_rows(self, rows, key):
        """
        Deduplicate a list of row dicts by a given key.

        Parameters
        ----------
        rows : list of dict
            Row dicts to deduplicate
        key : str
            Dict key to deduplicate on

        Returns
        -------
        list of dict
            Deduplicated list of rows
        """
        seen = set()
        unique_rows = []

        for row in rows:
            row_key = row.get(key)

            if row_key is None:
                unique_rows.append(row)
                continue

            if row_key not in seen:
                seen.add(row_key)
                unique_rows.append(row)

        return unique_rows

    # ----------------------------------------------------------------------
    # API Helpers
    # ----------------------------------------------------------------------
    def _request(self, endpoint_or_url, params=None):
        """
        Execute GET request against the OSF API.

        Parameters
        ----------
        `endpoint_or_url` : str
            Either a relative OSF API endpoint (e.g. "nodes/8r2b3/") or a
            full URL (e.g. a "related" link already returned by OSF).
        `params` : dict, optional
            Query parameters for the request.

        Returns
        -------
        dict
            Parsed JSON response from the OSF API.
        """
        if str(endpoint_or_url).startswith("http"):
            url = endpoint_or_url
        else:
            url = f"{self.base_url}/{str(endpoint_or_url).lstrip('/')}"

        try:
            response = requests.get(
                url,
                params=params,
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTP error while calling OSF: {e}") from e

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed: {e}") from e

        except ValueError as e:
            raise RuntimeError(f"Invalid JSON response: {e}") from e

    def _fetch_dataset(self, node_id, node_data):
        """
        Build a dataset-level (node) metadata row for a single OSF node.

        Parameters
        ----------
        node_id : str
            OSF node id
        node_data : dict
            Full parsed JSON response from `nodes/{node_id}/` (all 3 blocks:
            attributes, relationships, links)

        Returns
        -------
        dict
            Flattened dataset metadata row
        """
        node = node_data.get("data") or {}
        attrs = node.get("attributes", {}) or {}
        links = node.get("links", {}) or {}

        return {
            "node_id": node_id,
            "title": attrs.get("title"),
            "description": attrs.get("description"),
            "category": attrs.get("category"),
            "date_created": attrs.get("date_created"),
            "date_modified": attrs.get("date_modified"),
            "registration": attrs.get("registration"),
            "collection": attrs.get("collection"),
            "tags": attrs.get("tags"),
            "analytics_key": attrs.get("analytics_key"),
            "public": attrs.get("public"),
            "subjects": attrs.get("subjects"),
            "license": attrs.get("node_license"),
            "html": links.get("html"),
            "self": links.get("self"),
            "iri": links.get("iri"),
            "raw_attributes": node,
        }

    def _fetch_relationships(self, node_id, node_data):
        """
        Flatten a node's `relationships` block into rows, one per pointer.

        Excludes 'files' since that relationship is already followed and
        expanded into the full `files` table separately.

        Parameters
        ----------
        node_id : str
            OSF node id
        node_data : dict
            Full parsed JSON response from `nodes/{node_id}/`

        Returns
        -------
        list of dict
            One row per relationship pointer on this node
        """
        node = node_data.get("data") or {}
        relationships = node.get("relationships", {}) or {}

        rows = []
        for rel_name, rel_value in relationships.items():
            if rel_name == "files":
                continue

            href = (rel_value.get("links", {}) or {}).get("related", {}).get("href") \
                or (rel_value.get("links", {}) or {}).get("self", {}).get("href")

            rows.append({
                "node_id": node_id,
                "relationship_name": rel_name,
                "href": href,
                "has_inline_data": "data" in rel_value,
            })

        return rows

    def _get_provider_root_listing(self, node_id, provider):
        """
        Return the OSF API "related" listing URL for a node's storage provider.

        Parameters
        ----------
        node_id : str
            OSF node id
        provider : str
            Storage provider name (e.g. "osfstorage")

        Returns
        -------
        str
            The related listing URL for this node's storage provider
        """
        data = self._request(f"nodes/{node_id}/files/")

        for prov in data.get("data", []):
            if (prov.get("attributes", {}) or {}).get("provider") == provider:
                return prov["relationships"]["files"]["links"]["related"]["href"]

        raise ValueError(f"Provider {provider!r} not found for node {node_id!r}")

    def _fetch_files(self, node_id, provider, include_ext, exclude_ext):
        """
        Recursively walk a node's file tree (DFS through folders) and return
        flattened file-metadata rows, applying extension filters.

        Parameters
        ----------
        node_id : str
            OSF node id
        provider : str
            Storage provider name (e.g. "osfstorage")
        include_ext : list of str or None
            Only include files with these extensions
        exclude_ext : list of str or None
            Exclude files with these extensions

        Returns
        -------
        list of dict
            Flattened file metadata rows
        """
        listing_url = self._get_provider_root_listing(node_id, provider)

        rows = []
        stack = [listing_url]

        while stack:
            url = stack.pop()
            params = {"page[size]": 100}

            while url:
                page = self._request(url, params=params)
                params = None  # OSF's "next" link already carries page[size] forward

                for item in page.get("data", []):
                    attrs = item.get("attributes", {}) or {}
                    kind = attrs.get("kind")

                    if kind == "folder":
                        child = ((item.get("relationships") or {}).get("files") or {}) \
                            .get("links", {}).get("related", {}).get("href")
                        if child:
                            stack.append(child)

                    elif kind == "file":
                        name = attrs.get("name", "")
                        ext = f".{name.rsplit('.', 1)[-1].lower()}" if "." in name else ""

                        if include_ext and ext not in include_ext:
                            continue
                        if exclude_ext and ext in exclude_ext:
                            continue

                        rows.append({
                            "node_id": node_id,
                            "osf_file_id": item.get("id"),
                            "name": name,
                            "materialized_path": attrs.get("materialized_path"),
                            "provider": attrs.get("provider", provider),
                            "size_bytes": attrs.get("size"),
                            "download_url": (item.get("links") or {}).get("download"),
                            "raw_attributes": attrs,
                        })

                url = page.get("links", {}).get("next")

        return rows

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

        # Preserve column order while capturing all keys across rows
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
    def num_tables(self, **kwargs):
        """
        Prints the number of tables currently loaded OR returns the row count
        of a specific table.

        Maglab backend has up to 3 tables:
            - datasets: Dataset metadata. One row per OSF node retrieved from OSF
            - files: Combined file metadata from all requested nodes
            - relationships: Combined relationship pointers from all requested nodes

        Returns
        -------
        - If kwargs is empty: None
        - If kwargs has key 'table_name' with value 'datasets', 'files', or
          'relationships': returns row count of that table as an int
        """
        table_name = kwargs.get("table_name")
        if isinstance(table_name, str) and table_name.strip().lower() in ("datasets", "files", "relationships"):
            if not self._loaded:
                return 0

            table = self._cache.get(table_name.strip().lower(), {})
            if not table:
                return 0

            first_col = next(iter(table.values()), [])
            return len(first_col)

        if not self._loaded:
            print("Maglab Backend has 0 tables")
            return

        table_count = sum(1 for name in ("datasets", "files", "relationships") if self._cache.get(name))
        print(f"Database now has {table_count} tables")

    def get_table(self, table_name, dict_return=False):
        """
        Returns all data from a specified table.

        Parameters
        ----------
        table_name : str
            Must be 'datasets', 'files', or 'relationships'
        dict_return : bool, default False
            If True, returns OrderedDict (raw collection).
            If False (default), returns pandas DataFrame.

        Returns
        -------
        OrderedDict or pandas.DataFrame
            Depends on dict_return parameter
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

    def get_schema(self, table_name=None):
        """
        Returns schema information for all tables or a specific table in
        SQLite CREATE TABLE format.

        Shows table structure with column names and types (INTEGER, REAL, TEXT,
        OBJECT, etc.) similar to SQL backends.

        Parameters
        ----------
        table_name : str, optional
            If provided, returns schema for only that table.
            If None (default), returns schema for all tables.

        Returns
        -------
        str
            SQL-style CREATE TABLE statements
        """
        if not self._loaded or not self._cache:
            return "-- No tables loaded\n"

        def _table_schema(name, table_data):
            df = pd.DataFrame(table_data)
            lines = [f"CREATE TABLE {name} ("]
            column_defs = []
            for column in df.columns:
                pandas_dtype = str(df[column].dtype).lower()
                if 'int' in pandas_dtype:
                    sql_type = 'INTEGER'
                elif 'float' in pandas_dtype:
                    sql_type = 'REAL'
                elif pandas_dtype == 'bool':
                    sql_type = 'BOOLEAN'
                elif pandas_dtype == 'datetime64[ns]':
                    sql_type = 'DATETIME'
                elif pandas_dtype == 'object':
                    non_null = df[column].dropna()
                    sql_type = 'TEXT' if (non_null.empty or all(isinstance(x, str) for x in non_null)) else 'OBJECT'
                else:
                    sql_type = 'OBJECT'
                column_defs.append(f"    {column} {sql_type}")
            lines.append(",\n".join(column_defs))
            lines.append(");")
            return "\n".join(lines)

        if table_name is not None:
            if table_name not in self._cache:
                return f"-- Table '{table_name}' not found\n"
            table_data = self._cache.get(table_name)
            if not table_data:
                return f"-- Table '{table_name}' is empty\n"
            return _table_schema(table_name, table_data)

        schema_lines = ["-- Maglab Backend Schema", "-- (Read-only OSF metadata backend)", ""]
        for tbl_name, table_data in self._cache.items():
            if not table_data:
                continue
            schema_lines.append(_table_schema(tbl_name, table_data))
            schema_lines.append("")

        return "\n".join(schema_lines)

    # ----------------------------------------------------------------------
    # Query Interface (in-memory)
    # ----------------------------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query not supported for Maglab backend (non-SQL backend).

        Maglab is a read-only metadata backend that does not support SQL queries.
        Use find(), search(), or find_relation() for searching data instead.

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
            Always raised as Maglab does not support SQL queries
        """
        raise NotImplementedError(
            "query() is not supported for Maglab backend - it is a non-SQL backend.\n\n"
            "Use these methods instead:\n"
            "  dsi.find(...)             # Search for values\n"
            "  dsi.find_relation(...)    # Filter rows by a column relation\n"
        )

    def find_relation(self, column_name, relation, **kwargs):
        """
        Find rows where column matches relation (e.g., 'size_bytes > 5').

        Parameters
        ----------
        column_name : str
            Name of the column to apply the relation to
        relation : str
            Operator and value to apply to the column.
            Ex: >4, <4, =4, >=4, <=4, ==4, !=4, (4,5), ~4, ~~4

        Returns
        -------
        list of ValueObject
            One ValueObject per matching row
        """
        if not self._loaded:
            raise RuntimeError(
                "find_relation() ERROR: Cannot search an empty backend. "
                "Ensure data is loaded first."
            )

        operator, value = self._parse_relation(relation)
        matches = []

        for table_name in self.list(collection=True):
            df = self.get_table(table_name, dict_return=False)

            if column_name not in df.columns:
                continue

            filtered = self._apply_pandas_filter(df, column_name, operator, value)

            for idx, row in filtered.iterrows():
                vo = ValueObject()
                vo.t_name = table_name
                vo.c_name = list(df.columns)
                vo.row_num = int(idx) + 1
                vo.value = row.tolist()
                vo.type = "relation"
                matches.append(vo)

        return matches

    def _apply_pandas_filter(self, df, column, operator, value):
        """
        Apply a parsed DSI relation to a DataFrame column.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to filter
        column : str
            Column name to apply the filter to
        operator : str
            Parsed relation operator (e.g. '>', '==', 'contains', 'range')
        value : any
            Parsed relation value to compare against

        Returns
        -------
        pandas.DataFrame
            Filtered DataFrame
        """
        series = df[column]

        if operator == "contains":
            mask = series.astype(str).str.contains(str(value), case=False, na=False)
            return df[mask]

        if operator == "range":
            min_val, max_val = value
            if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                numeric = pd.to_numeric(series, errors="coerce")
                mask = numeric.between(min_val, max_val)
            else:
                text = series.astype("string")
                mask = (text >= str(min_val)) & (text <= str(max_val))
            return df[mask.fillna(False)]

        if value is None:
            if operator == "==":
                return df[series.isna()]
            if operator == "!=":
                return df[series.notna()]

        if isinstance(value, bool):
            if operator == "==":
                return df[series == value]
            if operator == "!=":
                return df[series != value]

        if operator in {"==", "!="}:
            if pd.api.types.is_numeric_dtype(series):
                compare_value = value
            else:
                compare_value = str(value)
                series = series.astype("string")

            mask = series == compare_value if operator == "==" else series != compare_value
            return df[mask.fillna(False)]

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            compare_series = pd.to_numeric(series, errors="coerce")
        else:
            compare_series = series.astype("string")
            value = str(value)

        if operator == ">":
            mask = compare_series > value
        elif operator == "<":
            mask = compare_series < value
        elif operator == ">=":
            mask = compare_series >= value
        elif operator == "<=":
            mask = compare_series <= value
        else:
            raise ValueError(f"Unsupported relation operator: {operator}")

        return df[mask.fillna(False)]

    def _parse_relation(self, relation):
        """
        Parse relation string into operator and value.

        Examples:
            '> 5' -> ('>', 5)
            '<= 10' -> ('<=', 10)
            '== 3' -> ('==', 3)
            "(2, 5)" -> ('range', (2, 5))
            "~~ 'climate'" -> ('contains', 'climate')

        Parameters
        ----------
        relation : str
            Relation string to parse

        Returns
        -------
        tuple
            (operator, value)
        """
        relation = relation.strip()

        if relation.startswith(">="):
            return ">=", self._parse_value(relation[2:])
        if relation.startswith("<="):
            return "<=", self._parse_value(relation[2:])
        if relation.startswith("=="):
            return "==", self._parse_value(relation[2:])
        if relation.startswith("!="):
            return "!=", self._parse_value(relation[2:])
        if relation.startswith("~~"):
            return "contains", self._parse_value(relation[2:])
        if relation.startswith(">"):
            return ">", self._parse_value(relation[1:])
        if relation.startswith("<"):
            return "<", self._parse_value(relation[1:])
        if relation.startswith("="):
            return "==", self._parse_value(relation[1:])
        if relation.startswith("~"):
            return "contains", self._parse_value(relation[1:])

        if relation.startswith("(") and relation.endswith(")"):
            values = relation[1:-1].split(",")
            if len(values) == 2:
                return "range", (self._parse_value(values[0]), self._parse_value(values[1]))

        raise ValueError(f"Unknown relation format: {relation}")

    def _parse_value(self, value):
        """
        Convert string to appropriate Python type.

        Parameters
        ----------
        value : str
            Value string to convert

        Returns
        -------
        int, float, bool, None, or str
            Converted value
        """
        value = str(value).strip()

        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]

        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() in {"none", "null"}:
            return None

        try:
            if "." not in value:
                return int(value)
            return float(value)
        except ValueError:
            return value

    # ----------------------------------------------------------------------
    # Artifact Processing
    # ----------------------------------------------------------------------
    def process_artifacts(self):
        """
        Returns all cached tables.

        Structure is:

        .. code-block:: text

            {
                "datasets": <datasets table>,
                "files": <files table>,
                "relationships": <relationships table>,
            }

        Useful for exporting or writing data to external systems.

        Returns
        -------
        OrderedDict
            All cached tables
        """
        if not self._loaded:
            return {}

        return self._cache

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

    def find_cell(self, query_object, row=False, **kwargs):
        """
        Finds all cells that match the given query_object.

        Matching behavior:
            - Exact match for all data types
            - Case-insensitive partial match for strings
            - String representation match for complex objects (dict, list)

        Parameters
        ----------
        `query_object` : int, float, or str
            The value to search for within table cells
        `row` : bool, optional, default=False
            If True, return the entire row containing the matching cell.
            If False, return only the matching cell.
        `**kwargs` : dict
            Additional keyword arguments

        Returns
        -------
        list of ValueObject
            One ValueObject per matching cell

        Notes
        -----
        ValueObject Structure:
            - t_name : table name
            - c_name :
                - row=False: list containing matched column name
                - row=True: list of all column names
            - row_num : row index of match
            - value :
                - row=False: matched cell value
                - row=True: full row of values
            - type :
                - row=False: 'cell'
                - row=True: 'row'
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

            for row_idx, row_data in enumerate(rows):
                for col_idx, cell in enumerate(row_data):
                    match = False

                    if query_object == cell:
                        match = True
                    elif is_str_query and isinstance(cell, str) and query_lower in cell.lower():
                        match = True
                    elif is_str_query and isinstance(cell, (dict, list, tuple)) and query_lower in str(cell).lower():
                        match = True

                    if match:
                        val = ValueObject()
                        val.t_name = table_name
                        val.row_num = row_idx

                        if row:
                            val.c_name = cols
                            val.value = list(row_data)
                            val.type = "row"
                        else:
                            val.c_name = [cols[col_idx]]
                            val.value = cell
                            val.type = "cell"

                        matches.append(val)

        return matches

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

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"\nTable: {name}")
            print(f"  - num of columns: {len(df.columns)}")
            print(f"  - num of rows: {len(df)}")

        print()

    def summary(self, table_name=None):
        """
        Returns detailed column-level statistics for tables.

        Parameters
        ----------
        table_name : str, optional
            If provided, returns summary for that table.
            Must be 'datasets', 'files', or 'relationships'.

        Returns
        -------
        pandas.DataFrame or list
            - If table_name specified: single DataFrame
            - If table_name=None: [table_names, df1, df2, ...]

        Notes
        -----
        - For OBJECT columns: min/max are lexicographic (alphabetical) for short text
        - Skips min/max for: long text (>80 chars), URLs, and raw/metadata columns
        - Numeric columns get full statistics: min, max, avg, std_dev
        """
        skip_min_max = {"raw_attributes", "download_url"}

        def _summarize(df):
            headers = ["column", "type", "unique", "min", "max", "avg", "std_dev"]
            rows = []

            for column in df.columns:
                series = df[column]
                non_null = series.dropna()

                if pd.api.types.is_bool_dtype(series):
                    column_type = "BOOLEAN"
                elif pd.api.types.is_integer_dtype(series):
                    column_type = "INTEGER"
                elif pd.api.types.is_float_dtype(series):
                    column_type = "REAL"
                elif pd.api.types.is_datetime64_any_dtype(series):
                    column_type = "DATETIME"
                elif non_null.empty or all(isinstance(v, str) for v in non_null):
                    column_type = "TEXT"
                else:
                    column_type = "OBJECT"

                has_complex_values = (
                    non_null.apply(lambda v: isinstance(v, (dict, list, tuple, set))).any()
                    if not non_null.empty else False
                )

                unique_vals = int(non_null.astype(str).nunique()) if has_complex_values else int(non_null.nunique())

                min_val = max_val = avg_val = std_dev = None

                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
                if not non_null.empty and len(numeric_series) == len(non_null):
                    min_val = numeric_series.min()
                    max_val = numeric_series.max()
                    avg_val = numeric_series.mean()
                    std_dev = numeric_series.std()
                    if pd.isna(std_dev):
                        std_dev = None
                elif (
                    not non_null.empty
                    and not has_complex_values
                    and column.lower() not in skip_min_max
                    and non_null.astype(str).str.len().max() <= 80
                ):
                    try:
                        min_val = non_null.min()
                        max_val = non_null.max()
                    except TypeError:
                        pass

                rows.append([column, column_type, unique_vals, min_val, max_val, avg_val, std_dev])

            return pd.DataFrame(rows, columns=headers, dtype=object)

        if table_name is not None:
            if table_name not in self._cache:
                raise ValueError(f"Table '{table_name}' not found. Available tables: {list(self._cache.keys())}")
            if not self._cache[table_name]:
                raise ValueError(f"Table '{table_name}' is empty")
            return _summarize(pd.DataFrame(self._cache[table_name]).infer_objects())

        table_names = []
        summary_dfs = []
        for name, table in self._cache.items():
            if not table:
                continue
            table_names.append(name)
            summary_dfs.append(_summarize(pd.DataFrame(table).infer_objects()))

        return [table_names] + summary_dfs

    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Displays rows from a specified table.

        By default, shows ALL columns (including raw_* columns) with FULL content.
        You can optionally specify a subset of columns to display.

        Parameters
        ----------
        `table_name` : str
            Name of the table to display ('datasets', 'files', or 'relationships')
        `num_rows` : int, default 25
            Number of rows to display. Set to None to show all rows.
        `display_cols` : list of str, optional
            Specific columns to display. If None, shows all columns.

        Returns
        -------
        pandas.DataFrame
            Displayed table data with long strings truncated
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        if table_name not in self._cache:
            raise ValueError(
                f"Table '{table_name}' not found. "
                f"Available tables: {list(self._cache.keys())}"
            )

        table = self._cache.get(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' is empty")

        df = pd.DataFrame(table)

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in '{table_name}': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        df.attrs["max_rows"] = len(df)

        if num_rows:
            df = df.head(num_rows)

        return df.map(
            lambda x: (str(x)[:60] + "..." if isinstance(x, str) and len(str(x)) > 60 else x)
        )

    def notebook(self, **kwargs):
        """
        Notebook generation not supported for Maglab backend.
        """
        raise NotImplementedError("Notebook generation not supported for Maglab backend")

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    def close(self):
        """
        Resets backend state and clears all cached data.
        """
        self._cache = OrderedDict()
        self._loaded = False

    # ----------------------------------------------------------------------
    # Abstract Methods
    # ----------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest not supported for Maglab backend (read-only).

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
            Always raised as Maglab backend is read-only
        """
        raise NotImplementedError("Maglab backend is read-only")
