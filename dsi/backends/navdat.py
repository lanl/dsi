"""NAVDAT webserver backend for DSI.

The backend queries the EarthChem PetDB v4 API and exposes sample and
citation metadata as in-memory DSI tables. NAVDAT data is served through
PetDB because navdat.org does not provide an API.
"""

from collections import OrderedDict
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests

from dsi.backends.webserver import Webserver


class ValueObject:
    """Container for results returned by the find methods."""
    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""


class NAVDAT(Webserver):
    """Read-only PetDB v4 backend for NAVDAT geochemistry metadata.

    The ``samples`` table contains one row per sample. The ``citations``
    table contains one row per citation or dataset record.
    """
    read_only = True

    def __init__(self, url=None, params=None, **kwargs):
        """Initialize the backend and optionally load PetDB data.

        Parameters
        ----------
        `url` : str, optional
            Base API URL. Defaults to ``https://api.earthchem.org``.
        `params` : dict or list of dict, optional
            Search properties passed as GET query parameters to both
            `/v4/locations/samples` and `/v4/citations`.
        `**kwargs` : dict
            Additional keyword arguments:
                - verify_ssl: enable TLS certificate verification.
                - fetch_citations: populate the ``citations`` table.
        """

        DEFAULT_URL = "https://api.earthchem.org"

        base_url = url or DEFAULT_URL

        self.verify_ssl = kwargs.get("verify_ssl", True)
        self.fetch_citations = kwargs.get("fetch_citations", True)

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")

        # Skip data retrieval when only validating the endpoint.
        if kwargs.get("only_validate", False):
            return

        self._cache = OrderedDict()

        self._loaded = False
        self.params = params or {}
        self.validate_error_msg = None

        if not self.validate_connection():
            self._loaded = False
            raise ConnectionError(self.validate_error_msg or "Failed to connect to the PetDB v4 API")

        if self.params:
            try:
                self._load_initial_data(self.params)
                self._loaded = True
            except Exception as e:
                self._loaded = False
                raise RuntimeError(f"Failed to load initial data: {e}") from e
        else:
            self._loaded = True

    def validate_connection(self):
        """Return whether the PetDB v4 metrics endpoint is available."""
        try:
            response = requests.get(
                f"{self.base_url}/v4/metrics",
                verify=self.verify_ssl,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict) or data.get("status") != "success":
                self.validate_error_msg = f"Unexpected response envelope from {self.base_url}/v4/metrics: {data}"
                return False

            return True

        except requests.exceptions.Timeout:
            self.validate_error_msg = f"Connection timeout: Cannot reach {self.base_url} within 10 seconds"
            return False
        except requests.exceptions.ConnectionError:
            self.validate_error_msg = f"Connection failed: Cannot connect to {self.base_url}. Check your network connection."
            return False
        except requests.exceptions.HTTPError as e:
            self.validate_error_msg = f"HTTP {e.response.status_code} Error from {self.base_url}: {str(e)}"
            return False
        except requests.exceptions.RequestException as e:
            self.validate_error_msg = f"Failed to validate connection to {self.base_url}: {str(e)}"
            return False
        except Exception:
            return False

    def _load_initial_data(self, params):
        """Load and deduplicate sample and citation data for one or more queries."""
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")

        all_sample_rows = []
        all_citation_rows = []

        for query_params in query_list:
            all_sample_rows.extend(self._fetch_samples(query_params))
            if self.fetch_citations:
                all_citation_rows.extend(self._fetch_citations(query_params))

        unique_sample_rows = self._deduplicate_rows(all_sample_rows)
        unique_citation_rows = self._deduplicate_rows(all_citation_rows)

        self._cache["samples"] = self._rows_to_table(unique_sample_rows)
        if unique_citation_rows:
            self._cache["citations"] = self._rows_to_table(unique_citation_rows)

        self._loaded = True

    # Query parameters supported by the PetDB search endpoints.
    SEARCH_PROP_KEYS = [
        "sampleNames", "authors", "citationTitles", "journals",
        "publicationYears", "laboratories", "dataSources", "expeditions",
        "analysisTypes", "geoFeatures", "taxons", "variables",
        "boundingBox", "polygons", "precision",
    ]

    def _build_search_props(self, query_params):
        """Return supported PetDB search properties from a query."""
        return {k: query_params[k] for k in self.SEARCH_PROP_KEYS if k in query_params}

    def _fetch_samples(self, query_params):
        """Fetch sample groups and flatten them into individual sample rows."""
        rest_params = self._build_search_props(query_params)
        if "size" in query_params:
            rest_params["size"] = query_params["size"]

        try:
            response = requests.get(
                f"{self.base_url}/v4/locations/samples",
                params=rest_params,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Warning: Could not retrieve samples: {e}")
            return []

        if not isinstance(payload, dict) or payload.get("status") != "success":
            print(f"Warning: Unexpected response from /v4/locations/samples: {payload}")
            return []

        groups = payload.get("data", [])
        rows = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            point = group.get("point", {}) or {}
            group_fields = {
                "rootParent": group.get("rootParent"),
                "groupDocCount": group.get("docCount"),
                "sampleCount": group.get("sampleCount"),
                "groupLat": point.get("lat"),
                "groupLon": point.get("lon"),
            }
            for sample in group.get("samples", []) or []:
                if not isinstance(sample, dict):
                    continue
                geometry = sample.get("sampleGeometry", {}) or {}
                coords = geometry.get("coordinates", [None, None])
                row = dict(group_fields)
                row.update({
                    "sampleName": sample.get("sampleName"),
                    "sampleId": sample.get("sampleId"),
                    "sampleDocCount": sample.get("docCount"),
                    "sampleLon": coords[0] if len(coords) > 0 else None,
                    "sampleLat": coords[1] if len(coords) > 1 else None,
                })
                rows.append(row)

        return rows

    def _fetch_citations(self, query_params):
        """Fetch citation records and flatten nested values into strings."""
        rest_params = self._build_search_props(query_params)

        try:
            response = requests.get(
                f"{self.base_url}/v4/citations",
                params=rest_params,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Warning: Could not retrieve citations: {e}")
            return []

        if not isinstance(payload, dict) or payload.get("status") != "success":
            print(f"Warning: Unexpected response from /v4/citations: {payload}")
            return []

        records = payload.get("data", [])
        rows = []
        for record in records:
            if not isinstance(record, dict):
                continue

            authors = record.get("citationAuthors", []) or []
            author_str = "; ".join(a.get("fullName", "") for a in authors if isinstance(a, dict))

            methods = record.get("methods", []) or []
            method_str = ", ".join(m.get("methodName", "") for m in methods if isinstance(m, dict))

            identifiers = record.get("citationIdentifiers", []) or []
            doi_str = ", ".join(
                i.get("identifier", "") for i in identifiers
                if isinstance(i, dict) and i.get("identifierType") == "DOI"
            )

            rows.append({
                "citationId": record.get("citationId"),
                "citationTitle": record.get("citationTitle"),
                "citationCode": record.get("citationCode"),
                "citationContainerTitle": record.get("citationContainerTitle"),
                "citationPublicationYear": record.get("citationPublicationYear"),
                "citationVolume": record.get("citationVolume"),
                "citationIssue": record.get("citationIssue"),
                "citationPages": record.get("citationPages"),
                "citationAuthors": author_str,
                "citationDOIs": doi_str,
                "datasetCode": record.get("datasetCode"),
                "datasetTitle": record.get("datasetTitle"),
                "datasetNum": record.get("datasetNum"),
                "analysisType": record.get("analysisType"),
                "methods": method_str,
            })

        return rows

    def _deduplicate_rows(self, rows):
        """
        Removes exact-duplicate rows (by full content), since paginated
        multi-query fetches can overlap.

        Parameters
        ----------
        rows : list of dict

        Returns
        -------
        list of dict
        """
        seen = set()
        unique_rows = []
        for row in rows:
            key = tuple(sorted((k, str(v)) for k, v in row.items()))
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        return unique_rows

    def _rows_to_table(self, rows):
        """
        Convert list-of-dicts to column-oriented OrderedDict.

        Parameters
        ----------
        `rows` : list of dict

        Returns
        -------
        OrderedDict
            Column-oriented table structure
        """
        if not rows:
            return OrderedDict()

        cols = []
        for row in rows:
            for c in row.keys():
                if c not in cols:
                    cols.append(c)

        table = OrderedDict({c: [] for c in cols})
        for row in rows:
            for c in cols:
                table[c].append(row.get(c))

        return table

    # ----------------------------------------------------------------------
    # Terminal Methods
    # ----------------------------------------------------------------------
    def num_tables(self, **kwargs):
        """
        Prints the number of tables currently loaded, or returns the number
        of rows in a specific table if `table_name` is given.

        Returns
        -------
        - If kwargs is empty: None (prints count)
        - If kwargs has 'table_name': int (row count for that table, 0 if missing/empty)
        """
        table_name = kwargs.get("table_name")
        if isinstance(table_name, str):
            if not self._loaded:
                return 0
            table = self._cache.get(table_name, {})
            if not table:
                return 0
            first_col = next(iter(table.values()), [])
            return len(first_col)

        if not self._loaded:
            print("NAVDAT Backend has 0 tables")
            return

        table_count = sum(1 for t in self._cache.values() if t)
        print(f"Database now has {table_count} tables")

    def get_table(self, table_name, dict_return=False):
        """
        Returns all data from a specified table.

        Parameters
        ----------
        table_name : str
            Must be 'samples' or 'citations'
        dict_return : bool, default False
            If True, returns OrderedDict. If False, returns pandas DataFrame.
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

        return table if dict_return else pd.DataFrame(table)

    def get_schema(self, table_name: str | None = None):
        """
        Returns schema information for all tables or a specific table in
        SQLite CREATE TABLE format.
        """
        def infer_sql_type(series):
            pandas_dtype = str(series.dtype).lower()
            if 'int' in pandas_dtype:
                return 'INTEGER'
            elif 'float' in pandas_dtype:
                return 'REAL'
            elif pandas_dtype == 'bool':
                return 'BOOLEAN'
            elif pandas_dtype == 'datetime64[ns]':
                return 'DATETIME'
            non_null = series.dropna()
            if non_null.empty or all(isinstance(x, str) for x in non_null):
                return 'TEXT'
            return 'OBJECT'

        if not self._loaded or not self._cache:
            return "-- No tables loaded\n"

        tables_to_render = {table_name: self._cache[table_name]} if table_name else self._cache
        if table_name and table_name not in self._cache:
            return f"-- Table '{table_name}' not found\n"

        schema_lines = []
        for tbl_name, table_data in tables_to_render.items():
            if not table_data:
                continue
            df = pd.DataFrame(table_data)
            schema_lines.append(f"CREATE TABLE {tbl_name} (")
            column_defs = [f"    {col} {infer_sql_type(df[col])}" for col in df.columns]
            schema_lines.append(",\n".join(column_defs))
            schema_lines.append(");")
            schema_lines.append("")

        return "\n".join(schema_lines) if schema_lines else f"-- Table '{table_name}' is empty\n"

    # ----------------------------------------------------------------------
    # Query Interface (in-memory)
    # ----------------------------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query not supported for NAVDAT backend (non-SQL backend).

        Raises
        ------
        NotImplementedError
            Always raised - use find(), search(), or find_relation() instead.
        """
        raise NotImplementedError(
            "query() is not supported for NAVDAT backend - it is a non-SQL backend.\n\n"
            "Use these methods instead:\n"
            "  dsi.find('minage < 10')                     # Query with conditions\n"
            "  dsi.search('basalt')                         # Search across all tables\n"
            "  dsi.find_relation('samples', 'citations')    # Query relationships\n"
        )

    def find_relation(self, column_name, relation, **kwargs):
        """
        Find rows where column matches relation (e.g., 'minage > 5').
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

            if operator in {'>', '<', '>=', '<=', 'range'}:
                df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
                df = df.dropna(subset=[column_name])

            filtered = self._apply_pandas_filter(df, column_name, operator, value)

            for idx, row in filtered.iterrows():
                vo = ValueObject()
                vo.t_name = table_name
                vo.c_name = list(df.columns)
                vo.row_num = int(idx) + 1
                vo.value = row.tolist()
                vo.type = "cell"
                matches.append(vo)

        return matches

    def _apply_pandas_filter(self, df, column, operator, value):
        if operator == '>':
            return df[df[column] > value]
        elif operator == '<':
            return df[df[column] < value]
        elif operator == '>=':
            return df[df[column] >= value]
        elif operator == '<=':
            return df[df[column] <= value]
        elif operator == '==':
            return df[df[column] == value]
        elif operator == '!=':
            return df[df[column] != value]
        elif operator == 'contains':
            return df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
        elif operator == 'range':
            min_val, max_val = value
            return df[(df[column] >= min_val) & (df[column] <= max_val)]
        return pd.DataFrame()

    def _parse_relation(self, relation):
        relation = relation.strip()
        if relation.startswith('>='):
            return '>=', self._parse_value(relation[2:])
        elif relation.startswith('<='):
            return '<=', self._parse_value(relation[2:])
        elif relation.startswith('=='):
            return '==', self._parse_value(relation[2:])
        elif relation.startswith('!='):
            return '!=', self._parse_value(relation[2:])
        elif relation.startswith('~~'):
            return 'contains', self._parse_value(relation[2:])
        elif relation.startswith('>'):
            return '>', self._parse_value(relation[1:])
        elif relation.startswith('<'):
            return '<', self._parse_value(relation[1:])
        elif relation.startswith('(') and relation.endswith(')'):
            parts = relation[1:-1].split(',')
            if len(parts) == 2:
                return 'range', (self._parse_value(parts[0]), self._parse_value(parts[1]))
        raise ValueError(f"Unknown relation format: {relation}")

    def _parse_value(self, value_str):
        value_str = str(value_str).strip()
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            value_str = value_str[1:-1]
        try:
            if '.' not in value_str:
                return int(value_str)
            return float(value_str)
        except ValueError:
            return value_str

    # ----------------------------------------------------------------------
    # Artifact Processing
    # ----------------------------------------------------------------------
    def process_artifacts(self):
        """
        Returns all cached tables. Useful for exporting to external systems.

        Returns
        -------
        OrderedDict
        """
        if not self._loaded:
            return {}
        return self._cache

    # ----------------------------------------------------------------------
    # Find Methods
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """
        Searches for all instances of query_object across all tables, at
        the table, column, and cell levels.
        """
        query_str = str(query_object).lower()
        return (
            self.find_table(query_str) +
            self.find_column(query_str) +
            self.find_cell(query_object)
        )

    def find_table(self, query_object, **kwargs):
        """Finds all tables whose names contain the given query_object."""
        if not isinstance(query_object, str):
            raise TypeError("find_table() ERROR: query_object must be a string")

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

    def find_column(self, query_object, range=False, **kwargs):
        """Finds all columns whose names contain the given query_object."""
        if not isinstance(query_object, str):
            raise TypeError("find_column() ERROR: query_object must be a string")

        matches = []
        query = query_object.lower()

        for table_name, table_data in self._cache.items():
            for col_name, col_data in table_data.items():
                if query in col_name.lower():
                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col_name]
                    val.row_num = None
                    val.value = col_data
                    val.type = "column"

                    if range:
                        numeric_col = pd.to_numeric(
                            pd.Series(col_data),
                            errors="coerce",
                        ).dropna()

                        if not numeric_col.empty:
                            val.value = {
                                "min": numeric_col.min(),
                                "max": numeric_col.max(),
                            }

                    matches.append(val)

        return matches

    def find_cell(self, query_object, row=True, **kwargs):
        """
        Finds all cells that match the given query_object.

        Parameters
        ----------
        query_object : any
            Value to search for.
        row : bool, optional
            If True, return full matching rows.
            If False, return individual matching cells.
        """
        matches = []
        seen_rows = set()

        is_str_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str_query else None

        for table_name, table_data in self._cache.items():
            if not table_data:
                continue

            cols = list(table_data.keys())
            df = pd.DataFrame(table_data)

            if df.empty:
                continue

            for row_idx, row_data in df.iterrows():
                row_matched = False

                for col in cols:
                    cell = row_data[col]
                    match = False

                    if pd.isna(cell) and not is_str_query and pd.isna(query_object):
                        match = True
                    elif query_object == cell:
                        match = True
                    elif is_str_query and query_lower in str(cell).lower():
                        match = True
                    elif is_str_query and isinstance(cell, (dict, list, tuple)):
                        if query_lower in str(cell).lower():
                            match = True

                    if not match:
                        continue

                    if row:
                        row_matched = True
                        break

                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = [col]
                    val.row_num = int(row_idx)
                    val.value = cell
                    val.type = "cell"
                    matches.append(val)

                if row and row_matched:
                    key = (table_name, int(row_idx))

                    if key in seen_rows:
                        continue

                    seen_rows.add(key)

                    val = ValueObject()
                    val.t_name = table_name
                    val.c_name = cols
                    val.row_num = int(row_idx)
                    val.value = row_data.to_dict()
                    val.type = "row"
                    matches.append(val)

        return matches

    # ----------------------------------------------------------------------
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        Lists tables, or prints table names with dimensions.
        """
        if collection:
            return list(self._cache.keys())

        for name, table in self._cache.items():
            df = pd.DataFrame(table)
            print(f"Table: {name}")
            print(f"  - num of columns: {len(df.columns)}")
            print(f"  - num of rows: {len(df)}")
            print()

    def summary(self, table_name=None):
        """
        Returns detailed column-level statistics for tables.
        """
        def is_complex_value(value):
            return isinstance(value, (dict, list, tuple, set))

        def safe_to_python(value):
            if pd.isna(value):
                return None
            if isinstance(value, (np.integer, np.floating)):
                return value.item()
            return value

        def summarize_dataframe(df):
            rows = []
            for column in df.columns:
                original_series = df[column]
                non_null = original_series.dropna()
                has_complex_values = (
                    non_null.apply(is_complex_value).any() if not non_null.empty else False
                )
                safe_series = non_null.astype(str) if has_complex_values else non_null

                pandas_dtype = str(original_series.dtype).lower()
                if 'int' in pandas_dtype:
                    dtype = 'INTEGER'
                elif 'float' in pandas_dtype:
                    dtype = 'REAL'
                elif pandas_dtype == 'bool':
                    dtype = 'BOOLEAN'
                elif pandas_dtype == 'object':
                    dtype = 'TEXT' if (non_null.empty or all(isinstance(x, str) for x in non_null)) else 'OBJECT'
                else:
                    dtype = 'OBJECT'

                row = {
                    "column": column, "type": dtype,
                    "unique": int(safe_series.nunique()) if not safe_series.empty else 0,
                    "min": None, "max": None, "avg": None, "std_dev": None,
                }

                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
                if not non_null.empty and len(numeric_series) == len(non_null):
                    row["min"] = safe_to_python(numeric_series.min())
                    row["max"] = safe_to_python(numeric_series.max())
                    row["avg"] = safe_to_python(numeric_series.mean())
                    row["std_dev"] = safe_to_python(numeric_series.std())
                elif not non_null.empty and not has_complex_values:
                    try:
                        row["min"] = safe_to_python(non_null.min())
                        row["max"] = safe_to_python(non_null.max())
                    except TypeError:
                        pass

                rows.append(row)

            summary_df = pd.DataFrame(rows, columns=["column", "type", "unique", "min", "max", "avg", "std_dev"])
            return summary_df.replace({np.nan: None})

        if not self._loaded:
            return pd.DataFrame() if table_name else [[], pd.DataFrame()]

        if table_name:
            if table_name not in self._cache:
                raise ValueError(f"Table '{table_name}' not found. Available: {list(self._cache.keys())}")
            table = self._cache.get(table_name)
            if not table:
                raise ValueError(f"Table '{table_name}' is empty")
            return summarize_dataframe(pd.DataFrame(table).infer_objects())

        table_names, summary_dfs = [], []
        for name, table in self._cache.items():
            if not table:
                continue
            table_names.append(name)
            summary_dfs.append(summarize_dataframe(pd.DataFrame(table).infer_objects()))
        return [table_names] + summary_dfs

    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Return rows from a specified table as a DataFrame.
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        if not isinstance(table_name, str):
            raise TypeError("display() ERROR: Input 'table_name' must be a string")

        if display_cols is not None and not isinstance(display_cols, list):
            raise TypeError(
                "display() ERROR: Input 'display_cols' must be a list of column names or None"
            )

        if num_rows is not None and (not isinstance(num_rows, int) or num_rows <= 0):
            raise ValueError("display() ERROR: Input 'num_rows' must be a positive integer or None")

        if table_name not in self._cache:
            raise ValueError(f"Table '{table_name}' not found. Available: {list(self._cache.keys())}")

        table = self._cache.get(table_name)

        if not table:
            raise ValueError(f"Table '{table_name}' is empty")

        df = pd.DataFrame(table)

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)

            if missing_cols:
                raise ValueError(f"Columns not found in '{table_name}': {missing_cols}")

            df = df[display_cols]

        if num_rows:
            df = df.head(num_rows)

        return df

    def notebook(self, **kwargs):
        """Notebook generation not supported for NAVDAT backend."""
        raise NotImplementedError("Notebook generation not supported for NAVDAT backend")

    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    def close(self):
        """Resets backend state and clears all cached data."""
        self._cache = OrderedDict()
        self._loaded = False

    # ----------------------------------------------------------------------
    # Abstract Methods
    # ----------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest not supported for NAVDAT backend (read-only).

        Raises
        ------
        NotImplementedError
            Always raised as NAVDAT backend is read-only
        """
        raise NotImplementedError("NAVDAT backend is read-only")
