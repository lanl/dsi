"""
Oceans11 Webserver Backend for DSI

Read-only backend that pulls metadata from DSI-based oceans11.lanl.gov
data catalog and exposes it as in-memory DSI tables: datasets and resources.
"""

import re
import operator
import pandas as pd
from pathlib import Path
from collections import OrderedDict
from urllib.parse import urlparse

from dsi.backends.webserver import Webserver

import urllib3
from urllib.parse import urljoin
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
# Oceans11 Backend (Webserver - Read only)
# ----------------------------------------------------------------------
class Oceans11(Webserver):
    """
    DSI-based web backend for querying Oceans11 metadata in-memory
    """
    read_only = True
    # ----------------------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from DSI databases.

        Parameters
        ----------
        `url` : str, optional
            Base Oceans11 URL.
        `params` : dict, optional
            Dictionary of initial query parameters used to fetch data from OSTI.
            Supported keys:
                "q",
                "keyword",
                "osti_id",
                "title",
                "authors",
                "doi",
                "report_number",
                "rows"
        `**kwargs` : dict
            Additional keyword arguments:
                - workspace : str, optional
                    
        """         
        DEFAULT_URL = "https://oceans11.lanl.gov/dataCatalog/oceans11.db"
        base_url = url or DEFAULT_URL

       # ----------------------------------------------------------------------
        # Auth / connection config
        # ----------------------------------------------------------------------
        self.workspace = kwargs.get(
            "workspace",
            str(Path("./").expanduser()) #where the downloaded file will be saved
        )

        parsed = urlparse(base_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Oceans11 catalog URL must be http or https")

        self.base_url = base_url.rstrip("/")

        # Data storage (tiered structure)
        # Tier 1: datasets, Tier 2: per-dataset resource tables
        self._cache = OrderedDict()
        self._resource_tables = []
        self._dataset_id_map = {}
        self._dataset_title_map = {}

        self._loaded = False
        self.catalog_path = None # the local path for the T1 catalog. 
        self.params = params or {}

        # Validate connection FIRST before attempting to load data
        try:
            self.catalog_path = self.validate_connection()
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
            Validates that the base Oceans11 URL is accessible and functional.
            
            This method tests the connection by making a simple DSI pull_data call to:
                - Download the oceans11.db catalog from:
                https://oceans11.lanl.gov/dataCatalog/
                - Set self.catalog_path to the download location
            
            Raises
            ------
            ConnectionError
                If online catalog is inaccessible or pull_data failed
            RuntimeError
                If the downloaded catalog is corrupt or inaccessible
            
            Returns
            -------
            bool
                True if connection is valid
            """                
            try:
                from dsi.utils.federated.federate_datasets import pull_data

                info = pull_data(
                    location_type="url",
                    location=self.base_url,
                    path=self.base_url,
                    abs_path_workspace_folder=self.workspace,
                    host_username={},
                    download_limit = float("inf")
                )

                if info is None:
                    raise ConnectionError(f"Failed to download catalog from {self.base_url}")

                local_path = info.get("local_path")
                if not local_path or not Path(local_path).is_file():
                    raise RuntimeError("Downloaded catalog file is invalid or missing")

                return local_path

            except Exception as e:
                raise ConnectionError(f"Unable to access Oceans11 catalog: {e}") from e

    # ---------------------------------------------------
    # Initial Data Load
    # ---------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch datasets/resources from downloaded oceans11.db and store in memory.

        params can be:
            dict       -> one Oceans11 request
            list[dict] -> multiple Oceans11 requests merged into one records table
        """        
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")

        from dsi.dsi import DSI

        # load the local DSI db 
        self._catalog_dsi = DSI(
            filename=self.catalog_path,
            backend_name="Sqlite",
            silence_messages=True,
        )

        # collate results over multiple queries for T1
        all_records = []

        for query_params in query_list:
            records = self._run_single_query(query_params)
            all_records.extend(records)

        # remove any multiple entries that may arise from merge
        unique_records = self._deduplicate_records(all_records)

        url_column = "t2db_url"
        # Download/load T2 DBs only for selected records
        for record in unique_records:
            t2db_url = record.get(url_column)

            if not t2db_url:
                continue

            t2db_path = self._download_t2_db(t2db_url)
            record["t2db_path"] = t2db_path

            self._load_t2_tables(record, t2db_path)

        # Tier 1: selected rows only
        self._cache["records"] = self._rows_to_table(unique_records)

        self._dataset_id_map = {
            row.get("osti_id"): row
            for row in unique_records
            if row.get("osti_id") is not None
        }

        self._dataset_title_map = {
            row.get("title"): row
            for row in unique_records
            if row.get("title") is not None
        }

        self._loaded = True

    # ---------------------------------------------------
    # Data Load Helpers - T1
    # ---------------------------------------------------

    def _run_single_query(self, params):
        """
        Run one query against the local Oceans11 Tier 1 catalog DB.
        """
        # supported search params to enter 
        supported_params = {
            "q",
            "keyword",
            "osti_id",
            "title",
            "authors",
            "doi",
            "report_number",
            "rows",
        }

        unknown = set(params) - supported_params
        if unknown:
            raise ValueError(
                f"Unsupported Oceans11 search parameter(s): {sorted(unknown)}"
            )

        rows = int(params.get("rows", 20))

        clauses = []

        # supported fields to search over
        KEYWORD_SEARCH_FIELDS = [
            "title",
            "authors",
            "subjects",
            "description",
            "doi",
            "report_number",
        ]

        q = params.get("q") or params.get("keyword")
        if q:
            clauses.append(
                self._or_like_clause(q, KEYWORD_SEARCH_FIELDS)
            )

        exact_fields = ["osti_id", "doi", "report_number"]
        like_fields = ["title", "authors"]

        for field in exact_fields:
            value = params.get(field)
            if value is not None:
                clauses.append(f"{field} = '{self._escape_sql(value)}'")

        for field in like_fields:
            value = params.get(field)
            if value is not None:
                clauses.append(f"{field} LIKE '%{self._escape_sql(value)}%'")

        query = "SELECT * FROM records"

        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        query += f" LIMIT {rows}"

        df = self._catalog_dsi.query(query, collection=True)

        if df is None or df.empty:
            return []

        return df.to_dict(orient="records")
    
    def _deduplicate_records(self, records):
        """
        Deduplicate Oceans11 records using osti_id when available.
        """
        seen = set()
        unique_records = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            key = (
                rec.get("osti_id")
                or rec.get("doi")
                or rec.get("title")
            )

            if key is None:
                unique_records.append(rec)
                continue

            if key not in seen:
                seen.add(key)
                unique_records.append(rec)

        return unique_records
    
    def _or_like_clause(self, value, fields):
        value = self._escape_sql(value)
        parts = [f"{field} LIKE '%{value}%'" for field in fields]
        return "(" + " OR ".join(parts) + ")"

    def _escape_sql(self, value):
        return str(value).replace("'", "''")

    # ---------------------------------------------------
    # Data Load Helpers - T2
    # ---------------------------------------------------
    def _download_t2_db(self, t2db_url):
        """
        Download the T2 DBs identified from the search 
        """        
        from dsi.utils.federated.federate_datasets import pull_data

        base_url = "https://oceans11.lanl.gov/dataCatalog/"

        full_url = urljoin(base_url, t2db_url)

        info = pull_data(
            location_type="url",
            location=full_url,
            path=full_url,
            abs_path_workspace_folder=self.workspace,
            host_username={},
            download_limit=float("inf")
        )

        if info is None or not info.get("local_path"):
            raise RuntimeError(f"Failed to download T2 DB: {full_url}")

        return info["local_path"]
    
    def _load_t2_tables(self, record, t2db_path):
        """
        Load the downloaded T2 DBs to memory and link with the T1 db
        """        
        from dsi.dsi import DSI

        t2_dsi = DSI(
            filename=t2db_path,
            backend_name="Sqlite",
            silence_messages=True
        )

        table_names = t2_dsi.list(collection=True)

        dataset_key = (
            record.get("osti_id")
            or record.get("title")
        )

        for table_name in table_names:
            df = t2_dsi.get_table(table_name, collection=True)

            if df is None or df.empty:
                continue

            cache_table_name = f"{dataset_key}_{table_name}"

            self._cache[cache_table_name] = self._rows_to_table(
                df.to_dict(orient="records")
            )

            self._resource_tables.append(cache_table_name)    

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
 
    def _extract_tables(self, datasets):
        """
        Split selected Oceans11 Tier 1 records into dataset rows and lookup maps.
        Tier 2 tables are loaded separately from each selected row's t2db_path.

        Returns
        -------
        tuple
            (dataset_rows, resource_map, id_map)
        """

        dataset_rows = []
        resource_map = {}
        id_map = {}

        for ds in datasets:
            dataset_id = ds.get("osti_id") or ds.get("doi")
            dataset_title = ds.get("title") or dataset_id

            if dataset_id and dataset_title:
                id_map[dataset_id] = dataset_title

            # guaranteed fields to save
            row = {
                "osti_id": ds.get("osti_id"),
                "title": ds.get("title"),
                "authors": ds.get("authors"),
                "subjects": ds.get("subjects"),
                "description": ds.get("description"),
                "doi": ds.get("doi"),
                "report_number": ds.get("report_number"),
                "t2db_url": ds.get("t2db_url"),
                "t2db_path": ds.get("t2db_path"),
            }

            # Preserve any additional fields from the T1 DB without losing them
            for key, value in ds.items():
                if key not in row:
                    row[key] = value

            dataset_rows.append(row)

            # Placeholder for Tier 2 tables; populated elsewhere
            if dataset_title:
                resource_map.setdefault(dataset_title, [])

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


    # # ----------------------------------------------------------------------
    # # Terminal Methods
    # # ----------------------------------------------------------------------
    def num_tables(self):
        """
        Return the number of cached tables.
        """

        return len(self._cache)

    def get_table(self, table_name, dict_return=False):
        """
        Return a cached table by name.

        Parameters
        ----------
        table_name : str
            Name of the cached table.

        dict_return : bool, optional
            If True, return OrderedDict table directly.
            If False, return pandas DataFrame.
        """

        resolved_name = self._resolve_table_name(table_name)

        if resolved_name not in self._cache:
            raise ValueError(f"Table '{table_name}' not found")

        table = self._cache[resolved_name]

        if dict_return:
            return table

        return pd.DataFrame(table)

    def get_schema(self):
        """
        Return a lightweight schema description of cached tables from oceans11 server.
        """
        schema_lines = []
        for table_name, table in self._cache.items():
            cols = []
            for col_name, values in table.items():
                dtype = "TEXT"
                for v in values:
                    if v is None:
                        continue

                    if isinstance(v, bool):
                        dtype = "BOOLEAN"
                    elif isinstance(v, int):
                        dtype = "INTEGER"
                    elif isinstance(v, float):
                        dtype = "REAL"
                    break

                cols.append(f"    {col_name} {dtype}")

            create_stmt = (
                f"CREATE TABLE {table_name} (\n"
                + ",\n".join(cols)
                + "\n);"
            )
            schema_lines.append(create_stmt)

        return "\n\n".join(schema_lines)


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
        Not supported - Oceans11 backend is read-only.
        
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
            "Oceans11 backend is read-only. Cannot overwrite tables. "
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

        table_name = kwargs.get("table_name")

        if table_name:
            resolved = self._resolve_table_name(table_name)

            if resolved not in self._cache:
                raise ValueError(f"Table '{table_name}' not found")

            tables = {resolved: self._cache[resolved]}
        else:
            tables = self._cache

        results = {}

        for t_name, table in tables.items():
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

        # if not self._loaded:
        #     raise RuntimeError("Oceans11 backend is not loaded")

        # import sqlite3

        # conn = sqlite3.connect(":memory:")

        # try:
        #     # Load cached OrderedDict tables into temporary SQLite DB
        #     for table_name, table in self._cache.items():

        #         df = pd.DataFrame(table)

        #         df.to_sql(
        #             table_name,
        #             conn,
        #             if_exists="replace",
        #             index=False,
        #         )

        #     result = pd.read_sql_query(query, conn)

        # finally:
        #     conn.close()

        # if dict_return:
        #     return result

        # return result.to_dict(orient="records")


    # ----------------------------------------------------------------------
    # Artifact Processing (tiered table construction)
    # ----------------------------------------------------------------------
    def process_artifacts(self):
        """
        Return selected Tier 1 Oceans11 records for export/process.

        Tier 2 databases remain separate local files and are referenced
        through the `t2db_path` column.

        Returns
        -------
        OrderedDict
            Exportable Tier 1 records table
        """

        if not self._loaded:
            return {}

        # saving only the T1 table because it links to the T2 tables
        return OrderedDict({
            "records": self._cache["records"]
        })
    
        # If you want to save out all the T1 and T2 tables
        # return self._cache

    # ----------------------------------------------------------------------
    # Find Methods
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        if not self._loaded:
            return []

        results = []

        for table_name, table in self._cache.items():

            columns = list(table.keys())

            if not columns:
                continue

            num_rows = len(table[columns[0]])

            for row_idx in range(num_rows):

                row_values = []

                matched = False

                for col in columns:

                    value = table[col][row_idx]
                    row_values.append(value)

                    if query_object is None:
                        continue

                    if value is not None and str(query_object).lower() in str(value).lower():
                        matched = True

                if matched:
                    results.append(
                        ValueObject(
                            t_name=table_name,
                            c_name=columns,
                            row_num=row_idx + 1,
                            value=row_values,
                        )
                    )

        return results
    
    def find_table(self, query_object, **kwargs):
        """
        Find cached table names matching query_object.
        """

        if not self._loaded:
            return []

        results = []

        query = str(query_object).lower()

        for table_name in self._cache.keys():
            if query in table_name.lower():
                results.append(
                    ValueObject(
                        t_name=table_name,
                        c_name=["table_name"],
                        row_num=None,
                        value=[table_name],
                    )
                )

        return results

    def find_column(self, query_object, **kwargs):
        """
        Find columns matching query_object across cached tables.
        """

        if not self._loaded:
            return []

        results = []

        query = str(query_object).lower()

        for table_name, table in self._cache.items():

            for col_name, values in table.items():

                if query in col_name.lower():

                    results.append(
                        ValueObject(
                            t_name=table_name,
                            c_name=[col_name],
                            row_num=None,
                            value=values,
                        )
                    )

        return results

    def find_cell(self, query_object, row=False, **kwargs):
        """
        Find cells matching query_object across cached tables.
        """

        if not self._loaded:
            return []

        results = []

        query = str(query_object).lower()

        for table_name, table in self._cache.items():

            columns = list(table.keys())

            if not columns:
                continue

            num_rows = len(table[columns[0]])

            for row_idx in range(num_rows):

                row_values = []
                matched_cols = []

                for col in columns:

                    value = table[col][row_idx]
                    row_values.append(value)

                    if value is not None and query in str(value).lower():
                        matched_cols.append(col)

                if matched_cols:

                    results.append(
                        ValueObject(
                            t_name=table_name,
                            c_name=matched_cols if not row else columns,
                            row_num=row_idx + 1,
                            value=row_values if row else [
                                table[c][row_idx] for c in matched_cols
                            ],
                        )
                    )

        return results


    def find_relation(self, column_name, relation, **kwargs):
        """
        Find rows satisfying a column relation across cached tables.
        """

        if not self._loaded:
            return []

        ops = {
            "=": operator.eq,
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
        }

        relation = relation.strip()

        matched_op = None
        matched_value = None

        for op in sorted(ops.keys(), key=len, reverse=True):
            if relation.startswith(op):
                matched_op = op
                matched_value = relation[len(op):].strip().strip("'\"")
                break

        if matched_op is None:
            raise ValueError(f"Unsupported relation: {relation}")

        compare = ops[matched_op]

        results = []

        for table_name, table in self._cache.items():

            if column_name not in table:
                continue

            columns = list(table.keys())

            num_rows = len(table[column_name])

            for row_idx in range(num_rows):

                value = table[column_name][row_idx]

                try:
                    lhs = float(value)
                    rhs = float(matched_value)
                except Exception:
                    lhs = str(value)
                    rhs = str(matched_value)

                try:
                    if compare(lhs, rhs):

                        row_values = [
                            table[c][row_idx]
                            for c in columns
                        ]

                        results.append(
                            ValueObject(
                                t_name=table_name,
                                c_name=columns,
                                row_num=row_idx + 1,
                                value=row_values,
                            )
                        )

                except Exception:
                    continue

        return results

    # ----------------------------------------------------------------------
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        Lists cached Oceans11 tables.

        Parameters
        ----------
        `collection` : bool, default False
            If True, return list of table names.
            If False, print table names with dimensions and dataset IDs.
        """

        if collection:
            return self._cache.keys()

        for name, table in self._cache.items():
            df = pd.DataFrame(table)

            if name in self._resource_tables:
                print(f"{name} [T2]: ({len(df)} rows, {len(df.columns)} cols)")
            else:
                print(f"{name}: ({len(df)} rows, {len(df.columns)} cols)")


    def summary(self, table_name=None):
        """
        Returns table-level metadata for cached Oceans11 tables.

        If table_name is None:
            returns [table_names_list, df1, df2, ...]

        If table_name is provided:
            returns a single DataFrame
        """

        if not self._loaded:
            return pd.DataFrame()

        if table_name:
            resolved_name = self._resolve_table_name(table_name)
            table = self._cache.get(resolved_name)

            if not table:
                raise ValueError(f"Table '{resolved_name}' is empty or not found")

            df = pd.DataFrame(table)

            summary_dict = {
                "table_name": resolved_name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
            }

            if resolved_name in self._resource_tables:
                summary_dict["tier"] = "T2"
            else:
                summary_dict["tier"] = "T1"

            return pd.DataFrame([summary_dict])

        table_names = []
        summary_dfs = []

        for name, table in self._cache.items():
            df = pd.DataFrame(table)

            summary_dict = {
                "table_name": name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "tier": "T2" if name in self._resource_tables else "T1",
            }

            table_names.append(name)
            summary_dfs.append(pd.DataFrame([summary_dict]))

        return [table_names] + summary_dfs


    def display(self, table_name, num_rows=25, display_cols=None):
        """
        Displays rows from a specified Oceans11 table.

        Accepts either dataset_title or dataset_id for resource tables.

        Parameters
        ----------
        `table_name` : str
            Name or ID of the table to display

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

        # Store original row count before limiting rows
        df.attrs["max_rows"] = len(df)

        if num_rows:
            df = df.head(num_rows)

        # Truncate long strings for readable display
        df = df.map(
            lambda x:
                (str(x)[:60] + "...")
                if isinstance(x, str) and len(str(x)) > 60
                else x
        )

        return df


    def notebook(self, **kwargs):
        """
        Notebook generation not supported for Oceans11 backend.

        Parameters
        ----------
        `**kwargs` : dict
            Additional keyword arguments (unused)

        Returns
        -------
        None
        """
        pass


    # # ----------------------------------------------------------------------
    # # Lifecycle
    # # ----------------------------------------------------------------------
    def close(self):
        """
        Close Oceans11 backend and clear loaded state.
        """

        self._cache.clear()
        self._resource_tables.clear()
        self._dataset_id_map.clear()
        self._dataset_title_map.clear()

        if hasattr(self, "_catalog_dsi"):
            try:
                self._catalog_dsi.close()
            except Exception:
                pass

        self.catalog_path = None
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
            Always raised as Oceans11 backend is read-only
        """
        raise NotImplementedError("Oceans11 backend is read-only")
    