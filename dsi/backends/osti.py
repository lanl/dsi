"""
OSTI Backend for DSI

Read-only access that pulls metadata from REST-based OSTI backend
and exposes it as an in-memory DSI table: records
"""

import requests
import pandas as pd
from urllib.parse import urlparse
from collections import OrderedDict

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

# ---------------------------------------------------------
# OSTI Backend (Webserver - read only)
# ---------------------------------------------------------
class OSTI(Webserver):
    """
    REST-based web backend for querying OSTI metadata in-memory
    """
    read_only = True
    # ----------------------------
    # Initialization
    # ----------------------------    
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from REST API.

        `url` : str, optional
            Base OSTI URL. If None, a default OSTI endpoint is used.

        `params` : dict, optional
            Dictionary of initial query parameters used to fetch data from OSTI.

            Supported keys:
                - "q",
                - "osti_id",
                - "doi",
                - "fulltext",
                - "biblio",
                - "author",
                - "title",
                - "identifier",
                - "sponsor_org",
                - "research_org",
                - "contributing_org",
                - "source_id",
                - "publication_date_start",
                - "publication_date_end",
                - "entry_date_start",
                - "entry_date_end",
                - "language",
                - "country",
                - "site_ownership_code",
                - "subject",
                - "has_fulltext",
                - "sort",
                - "order",
                - "rows",
                - "page",
        
        `**kwargs` : dict
            Additional keyword arguments.

            - api_key : str, optional
                API key for authentication
            - verify_ssl : bool, optional
                Toggle SSL verification (default False)
        """        

        DEFAULT_URL = "https://www.osti.gov/api/v1"
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

        # OSTI public API does not require authentication,
        # but we allow optional API key for future or private endpoints
        self.headers = {}
        if self.api_key:
            self.headers["Authorization"] = self.api_key

        # skip data retrieval if only checking connection to oceans11
        if kwargs.get("only_validate", False):
            return

        # In-memory storage (DSI format)
        self._cache = OrderedDict()

        self._loaded = False
        self.params = params or {}
        self.validate_error_msg = None

        # Validate connection FIRST before attempting to load data
        if not self.validate_connection():
            self._loaded = False
            raise ConnectionError(self.validate_error_msg or f"Unable to connect to OSTI API at {self.base_url}")

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
    # ----------------------------------------------------------------------
    # Connection Validation
    # ----------------------------------------------------------------------
    def validate_connection(self):
        """
        Validates that the base OSTI URL is accessible and functional.
        
        Tests the connection by making an API call to verify:
            - URL is reachable
            - API responds with valid JSON
            - Response format is a list of records
        
        Return : bool
            True if connection is valid
            False if connection is invalid
        """
        try:
            # test_url = f"{self.base_url}/records"
            test_url = self.base_url + "/records"

            response = requests.get(
                test_url,
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=2,
                params={"rows": 1}  # minimal request

            )
            response.raise_for_status()

            data = response.json()

            # OSTI returns a list of records for /records
            if not isinstance(data, list):
                self.validate_error_msg = "OSTI connection error: response is not a list of records"
                return False

            return True

        except requests.RequestException as exc:
            self.validate_error_msg = f"OSTI connection error: {exc}"
            return False
        except Exception as exc:
                self.validate_error_msg = f"OSTI connection error: {exc}"
                return False


    # ---------------------------------------------------
    # Initial Data Load
    # ---------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch records from OSTI API and store in memory.

        params can be:
            dict       -> one OSTI request
            list[dict] -> multiple OSTI requests merged into one records table
        """

        if isinstance(params, dict):
            query_list = [params]

        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params

        else:
            raise TypeError("params must be a dict or a list of dicts")

        all_records = []

        for query_params in query_list:
            records = self._run_single_query(query_params)
            all_records.extend(records)

        unique_records = self._deduplicate_records(all_records)

        record_rows = self._extract_tables(unique_records)
        self._cache["records"] = self._rows_to_table(record_rows)

        self._loaded = True

    def _run_single_query(self, params):
        """
        Run one OSTI query and normalize the response to a list of records.
        """

        if "osti_id" in params and len(params) == 1:
            result = self._request(f"records/{params['osti_id']}")

            if isinstance(result, dict):
                return [result]

            if isinstance(result, list):
                return result

            return []

        request_params = self._build_request_params(params)

        result = self._request("records", request_params)

        if isinstance(result, list):
            return result

        if isinstance(result, dict):
            return [result]

        return []

    def _build_request_params(self, params):
        """
        Build OSTI /records query parameters from supported inputs.
        """

        supported_params = [
            "q",
            "osti_id",
            "doi",
            "fulltext",
            "biblio",
            "author",
            "title",
            "identifier",
            "sponsor_org",
            "research_org",
            "contributing_org",
            "source_id",
            "publication_date_start",
            "publication_date_end",
            "entry_date_start",
            "entry_date_end",
            "language",
            "country",
            "site_ownership_code",
            "subject",
            "has_fulltext",
            "sort",
            "order",
            "rows",
            "page",
        ]

        request_params = {
            "rows": params.get("rows", 20),
            "page": params.get("page", 1),
        }

        for key in supported_params:
            if key in ("rows", "page"):
                continue

            value = params.get(key)
            if value is not None:
                request_params[key] = value

        return request_params
    
    def _deduplicate_records(self, records):
        """
        Deduplicate OSTI records using osti_id when available.
        """

        seen = set()
        unique_records = []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            key = rec.get("osti_id") or rec.get("doi") or rec.get("title")

            if key is None:
                unique_records.append(rec)
                continue

            if key not in seen:
                seen.add(key)
                unique_records.append(rec)

        return unique_records    

# ---------------------------------------------------
# API Helpers
# ---------------------------------------------------   
    def _request(self, endpoint, params=None):
        """
        Execute GET request against OSTI API.

        Parameters
        ----------
        `endpoint` : str
            OSTI API endpoint name
        `params` : dict, optional
            Query parameters for the request

        Returns
        -------
        object
            Parsed JSON response from the OSTI API.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = requests.get(
                url,
                params=params or {},
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            self._loaded = False
            raise RuntimeError(f"HTTP error while calling OSTI: {e}") from e

        except requests.exceptions.RequestException as e:
            self._loaded = False
            raise RuntimeError(f"Request failed: {e}") from e 

        except ValueError as e:
            self._loaded = False
            raise RuntimeError(f"Invalid JSON response: {e}") from e
        

    def _flatten_list(self, values, key=None):
        """
        Flatten a list of values (strings or dicts) into a semicolon-separated string.
        """

        if not values:
            return ""

        flattened = []

        for item in values:
            if isinstance(item, dict):
                value = item.get(key) if key else None
                if value is None:
                    value = item.get("name") or item.get("value") or str(item)
            else:
                value = item

            if value is not None:
                flattened.append(str(value))

        return "; ".join(flattened)
    

    def _extract_tables(self, records):
        """
            Flatten OSTI dataset JSON into records tables.

            Parameters
            ----------
            `records` : list
                List of dataset records from OSTI API

            Returns
            -------
            'record_rows' : list of dict flattened dataset metadata

        """

        record_rows = []

        for rec in records:
            links = rec.get("links", []) or []

            citation_url = None
            doe_pages_url = None
            fulltext_url = None

            for link in links:
                if isinstance(link, dict):
                    rel = link.get("rel")
                    href = link.get("href")

                    if rel == "citation":
                        citation_url = href
                    elif rel == "citation_doe_pages":
                        doe_pages_url = href
                    elif rel == "fulltext":
                        fulltext_url = href

            record_rows.append({
                "osti_id": rec.get("osti_id"),
                "title": rec.get("title"),
                "doi": rec.get("doi"),
                "publication_date": rec.get("publication_date"),
                "entry_date": rec.get("entry_date"),
                "language": rec.get("language"),
                "country_publication": rec.get("country_publication"),
                "product_type": rec.get("product_type"),
                "description": rec.get("description"),
                "publisher": rec.get("publisher"),
                "journal_name": rec.get("journal_name"),
                "journal_volume": rec.get("journal_volume"),
                "journal_issue": rec.get("journal_issue"),
                "availability": rec.get("availability"),
                "format": rec.get("format"), 
                "report_number": rec.get("report_number"),
                "doe_contract_number": rec.get("doe_contract_number"),
                "nsa_number": rec.get("nsa_number"),
                "authors": self._flatten_list(rec.get("authors", []) or [], key="name"),
                "subjects": self._flatten_list(rec.get("subjects", []) or []),
                "sponsor_org": self._flatten_list(rec.get("sponsor_org", []) or [], key="name"),
                "research_org": self._flatten_list(rec.get("research_org", []) or [], key="name"),
                "contributor_org": self._flatten_list(rec.get("contributor_org", []) or [], key="name"),

                "has_fulltext": fulltext_url is not None,
                "citation_url": citation_url,
                "citation_doe_pages_url": doe_pages_url,
                "fulltext_url": fulltext_url,

                # "raw_links": links,
                "raw_record": rec,
            })

        return record_rows


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

        # Preserve column order while capturing all keys
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
    def num_tables(self):
        """
        Prints the number of tables in this backend.
        """
        table_count = len(self._cache)

        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")
    

    def get_table(self, table_name="records", dict_return=False):
        """
        Returns all data from the 'records' table
        
        `table_name` : str, optional, default='records'
            table_name must be 'records' or None
        `dict_return` : bool, default False
            If True, returns OrderedDict. 
            If False, returns DataFrame.
        
        **Return : OrderedDict or pandas.DataFrame**
        """
        if table_name != "records":
            raise ValueError("OSTI backend only contains the 'records' table")

        if "records" not in self._cache:
            raise ValueError("No OSTI records loaded")

        table = self._cache["records"]

        if dict_return:
            return table

        return pd.DataFrame(table)

    def get_schema(self):
        """
        Return a lightweight schema description of cached tables from OSTI.

        Return : str
            Each table's structural schema is combined into one large string.
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
                    elif isinstance(v, (dict, list, tuple, set)):
                        dtype = "OBJECT"
                    else:
                        dtype = "TEXT"

                    break

                cols.append(f"    {col_name} {dtype}")

            create_stmt = (
                f"CREATE TABLE {table_name} (\n"
                + ",\n".join(cols)
                + "\n);"
            )
            schema_lines.append(create_stmt)

        return "\n\n".join(schema_lines)


    # ---------------------------------------------------
    # Query Interface (in-memory)
    # ---------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Query not supported for OSTI backend (non-SQL backend).

        OSTI is a read-only metadata backend that does not support SQL queries.
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
            Always raised as OSTI does not support SQL queries.
        """
        raise NotImplementedError(
            "query() is not supported for OSTI backend - it is a non-SQL backend.\n\n"
            "Use these methods instead:\n"
            "  dsi.find(...)             # Search for values\n"
            "  dsi.search(...)           # Search across backend data\n"
        )


    # ----------------------------------------------------------------------
    # Artifact Processing (tiered table construction)
    # ----------------------------------------------------------------------
    def process_artifacts(self):
        """
        Returns all cached OSTI data::

            {
                "records": <records table>
            }

        Useful for exporting or writing data to external formats.

        Return : OrderedDict
            Cached records table
        """
        if not self._loaded:
            return {}

        return self._cache


    # ---------------------------------------------------
    # URL Validation
    # ---------------------------------------------------
    def validate_urls(self):
        """
        Validate URL fields in the records table.

        Adds boolean columns indicating whether each URL is reachable:
            - citation_url_valid
            - citation_doe_pages_url_valid
            - fulltext_url_valid
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot validate URLs.")

        if "records" not in self._cache:
            raise RuntimeError("No records table loaded.")

        df = pd.DataFrame(self._cache["records"])

        if df.empty:
            return

        url_fields = [
            "citation_url",
            "citation_doe_pages_url",
            "fulltext_url",
        ]

        for field in url_fields:
            if field not in df.columns:
                continue

            validity = []

            for url in df[field]:
                if not url:
                    validity.append(False)
                    continue

                try:
                    response = requests.head(
                        url,
                        allow_redirects=True,
                        timeout=5,
                        verify=self.verify_ssl,
                    )

                    # Some servers block HEAD → fallback to GET
                    if response.status_code == 405:
                        response = requests.get(
                            url,
                            timeout=5,
                            verify=self.verify_ssl,
                        )

                    validity.append(200 <= response.status_code < 400)

                except Exception:
                    validity.append(False)

            df[f"{field}_valid"] = validity

        # write back to cache
        self._cache["records"] = OrderedDict(
            (column, df[column].tolist())
            for column in df.columns
        )

    # ----------------------------------------------------------------------
    # Find Methods
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """
        Searches for all instances of `query_object` across the table, column, and cell levels.

        `query_object` : int, float, or str
            The value to search for across all tables in the backend
        
        `**kwargs` : dict
            Additional keyword arguments

        Return : list of ValueObjects representing matches across:
            - table names
            - column names
            - cell values

        ValueObject Structure:
            - t_name :  (str) Table name
            - c_name :  (list) Column name(s)
            - row_num : (int or None) Row index
            - value :   (any) Matched value or data
            - type :    (str) {'table', 'column', 'cell'}
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
        Finds all tables whose names contain the given query_object. Search is case-insensitive.

        `query_object` : str
            The string to match against table names
        `**kwargs` : dict
            Additional keyword arguments

        Return : list of ValueObject
            One ValueObject per matching table

        ValueObject Structure:
            - t_name :  (str) Table name
            - c_name :  (list) List of all columns in the table
            - value :   (dict) Full table data (dict of columns)
            - row_num : (None)
            - type :    (str) 'table'
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
        Finds all columns whose names contain the given query_object. Search is case-insensitive.

        `query_object` : str
            The string to match against column names
        `**kwargs` : dict
            Additional keyword arguments

        Return : list of ValueObject
            One ValueObject per matching column

        ValueObject Structure:
            - t_name :  (str) Table name
            - c_name :  (list) List with the matched column name
            - value :   (list) Full column data
            - row_num : (None)
            - type :    (str) 'column'
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

        Exact match for all data types, plus case-insensitive partial
        match for strings.

        `query_object` : int, float, or str
            The value to search for within table cells.

        `row` : bool, optional, default=False
            If True, return the entire row containing the matching cell.
            If False, return only the matching cell.

        `**kwargs` : dict
            Additional keyword arguments.

        Return : list of ValueObject
            One ValueObject per matching cell.

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

                    elif (
                        is_str_query
                        and isinstance(cell, str)
                        and query_lower in cell.lower()
                    ):
                        match = True

                    elif (
                        is_str_query
                        and isinstance(cell, (dict, list, tuple))
                        and query_lower in str(cell).lower()
                    ):
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


    def find_relation(self, column_name, relation, **kwargs):
        """
        Finds all rows in the records table that satisfy a relation
        applied to the given column.

        `column_name` : str
            Name of the column to apply the relation to.

        `relation` : str
            Operator and value to apply to the column.
            Ex: >4, <4, =4, >=4, <=4, ==4, !=4,
            (4,5), ~4, ~~4

        Return : list of ValueObject
            One ValueObject per matching row.

        ValueObject Structure:
            - t_name : table name
            - c_name : list of all columns
            - value : full row of values
            - row_num : 1-based row index
            - type : 'relation'
        """
        if not self._loaded or "records" not in self._cache:
            raise RuntimeError(
                "find_relation() ERROR: Cannot search an empty backend."
            )

        df = pd.DataFrame(self._cache["records"])

        if column_name not in df.columns:
            return (
                f"'{column_name}' is not a column in this database. "
                "Ensure the column is written first."
            )

        operator, value = self._parse_relation(relation)

        filtered = self._apply_pandas_filter(
            df,
            column_name,
            operator,
            value
        )

        if filtered.empty:
            return (
                f"Could not find any rows where "
                f" {column_name} {relation}  in this database."
            )

        matches = []

        for idx, row in filtered.iterrows():
            val = ValueObject()
            val.t_name = "records"
            val.c_name = list(df.columns)
            val.row_num = int(idx) + 1
            val.value = row.tolist()
            val.type = "relation"
            matches.append(val)

        return matches


    def _apply_pandas_filter(self, df, column, operator, value):
        """
        Apply a parsed DSI relation to a DataFrame column.
        """
        series = df[column]

        # Partial string match
        if operator == "contains":
            mask = (
                series.astype(str)
                .str.contains(str(value), case=False, na=False)
            )
            return df[mask]

        # Range comparison
        if operator == "range":
            min_val, max_val = value

            if (
                isinstance(min_val, (int, float))
                and not isinstance(min_val, bool)
                and isinstance(max_val, (int, float))
                and not isinstance(max_val, bool)
            ):
                numeric = pd.to_numeric(series, errors="coerce")
                mask = numeric.between(min_val, max_val)
            else:
                text = series.astype("string")
                mask = (
                    (text >= str(min_val))
                    & (text <= str(max_val))
                )

            return df[mask.fillna(False)]

        # Equality / inequality with null
        if value is None:
            if operator == "==":
                return df[series.isna()]

            if operator == "!=":
                return df[series.notna()]

        # Boolean comparison
        if isinstance(value, bool):
            if operator == "==":
                return df[series == value]

            if operator == "!=":
                return df[series != value]

        # Equality should preserve the stored column type.
        # This is important for OSTI fields such as osti_id, which
        # may contain numeric-looking values stored as strings.
        if operator in {"==", "!="}:
            if pd.api.types.is_numeric_dtype(series):
                compare_value = value
            else:
                compare_value = str(value)
                series = series.astype("string")

            if operator == "==":
                mask = series == compare_value
            else:
                mask = series != compare_value

            return df[mask.fillna(False)]

        # Ordering comparisons.
        # Use numeric comparison when the query value is numeric;
        # otherwise use string comparison.
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
        Parse a DSI relation into an operator and value.
        """
        relation = relation.strip()

        # Two-character operators must be checked first.
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

        # Single-character operators
        if relation.startswith(">"):
            return ">", self._parse_value(relation[1:])

        if relation.startswith("<"):
            return "<", self._parse_value(relation[1:])

        if relation.startswith("="):
            return "==", self._parse_value(relation[1:])

        if relation.startswith("~"):
            return "contains", self._parse_value(relation[1:])

        # Range
        if relation.startswith("(") and relation.endswith(")"):
            values = relation[1:-1].split(",")

            if len(values) == 2:
                return (
                    "range",
                    (
                        self._parse_value(values[0]),
                        self._parse_value(values[1]),
                    ),
                )

        raise ValueError(f"Unknown relation format: {relation}")


    def _parse_value(self, value):
        """
        Convert a DSI relation value to an appropriate Python value.
        """
        value = str(value).strip()

        if (
            (value.startswith("'") and value.endswith("'"))
            or
            (value.startswith('"') and value.endswith('"'))
        ):
            value = value[1:-1]

        # DSI escapes apostrophes for SQL using doubled quotes.
        value = value.replace("''", "'")

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
    # Utility / Display
    # ----------------------------------------------------------------------
    def list(self, collection=False):
        """
        Return a list of all tables and their dimensions from this OSTI backend.

        `collection` : bool, optional, default=False
            - If True, returns the list of table names.
            - If False (default), prints table names and dimensions for all tables.
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
        Returns numerical metadata for the cached OSTI records table.

        `table_name` : str, optional
            If specified, only the numerical metadata for that table is
            returned as a Pandas DataFrame.

            If None (default), names of all tables and metadata for each
            table are returned as a list:
            [table_name_list, table1_df, table2_df, ...]
        """
        if table_name is not None:
            if table_name not in self._cache:
                raise ValueError(
                    f"Table '{table_name}' not found. "
                    f"Available tables: {list(self._cache.keys())}"
                )

            if not self._cache[table_name]:
                raise ValueError(f"Table '{table_name}' is empty")

            return self._summary_helper(table_name)

        table_names = []
        summary_dfs = []

        for name, table in self._cache.items():
            if not table:
                continue

            table_names.append(name)
            summary_dfs.append(self._summary_helper(name))

        return [table_names] + summary_dfs


    def _summary_helper(self, table_name):
        """
        Internal helper for generating column-level summary metadata.
        """
        df = pd.DataFrame(self._cache[table_name]).infer_objects()

        headers = [
            "column",
            "type",
            "unique",
            "min",
            "max",
            "avg",
            "std_dev",
        ]

        skip_min_max = {
            "raw_record",
            "citation_url",
            "citation_doe_pages_url",
            "fulltext_url",
        }

        rows = []

        for column in df.columns:
            series = df[column]
            non_null = series.dropna()

            # Determine DSI-compatible column type
            if pd.api.types.is_bool_dtype(series):
                column_type = "BOOLEAN"
            elif pd.api.types.is_integer_dtype(series):
                column_type = "INTEGER"
            elif pd.api.types.is_float_dtype(series):
                column_type = "REAL"
            elif pd.api.types.is_datetime64_any_dtype(series):
                column_type = "DATETIME"
            elif non_null.empty or all(isinstance(value, str) for value in non_null):
                column_type = "TEXT"
            else:
                column_type = "OBJECT"

            # Complex values such as raw_record dictionaries are not hashable.
            has_complex_values = (
                non_null.apply(
                    lambda value: isinstance(value, (dict, list, tuple, set))
                ).any()
                if not non_null.empty
                else False
            )

            if has_complex_values:
                unique_vals = int(non_null.astype(str).nunique())
            else:
                unique_vals = int(non_null.nunique())

            min_val = None
            max_val = None
            avg_val = None
            std_dev = None

            # Calculate numerical statistics when the entire column is numeric.
            numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()

            if not non_null.empty and len(numeric_series) == len(non_null):
                min_val = numeric_series.min()
                max_val = numeric_series.max()
                avg_val = numeric_series.mean()
                std_dev = numeric_series.std()

                if pd.isna(std_dev):
                    std_dev = None

            # For short text columns, provide lexicographic min/max.
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

            rows.append([
                column,
                column_type,
                unique_vals,
                min_val,
                max_val,
                avg_val,
                std_dev,
            ])

        return pd.DataFrame(rows, columns=headers, dtype=object)




    def display(self, table_name="records", num_rows=25, display_cols=None):
        """
        Displays rows from the 'records' table.

        `table_name` : str, optional, default = 'records'
            Name of the table to display
        `num_rows` : int, default 25
            Number of rows to display
        `display_cols` : list of str, optional
            Subset of columns to display

        Return : pandas.DataFrame
            Displayed table data with long strings truncated
        """
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        if table_name != "records":
            raise ValueError("OSTI backend only contains the 'records' table")

        if "records" not in self._cache:
            raise ValueError("No OSTI records loaded")

        df = pd.DataFrame(self._cache["records"])

        if df.empty:
            raise ValueError("The records table is empty")

        if display_cols:
            missing_cols = set(display_cols) - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Columns not found in 'records': {missing_cols}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        df.attrs["max_rows"] = len(df)

        if num_rows:
            df = df.head(num_rows)

        return df.map(
            lambda x: (
                str(x)[:60] + "..."
                if isinstance(x, str) and len(str(x)) > 60
                else x
            )
        )


    def notebook(self, **kwargs):
        """
        **Notebook generation not supported for OSTI backend.**
        """
        raise NotImplementedError("Notebook generation not supported for OSTI backend")


    # ----------------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------------
    def close(self):
        """
        Reset backend state and clear cached data.
        """
        self._cache = OrderedDict()
        self._loaded = False


    # ----------------------------------------------------------------------
    # Abstract Methods
    # ----------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """
        Ingest is not supported for the OSTI backend.
        """
        raise NotImplementedError("OSTI backend is read-only")