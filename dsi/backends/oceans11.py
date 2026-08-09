"""
Oceans11 Webserver Backend for DSI

Read-only backend that pulls metadata from the DSI-based
https://oceans11.lanl.gov data catalog and exposes it as in-memory DSI tables.
"""

from collections import OrderedDict
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse

import pandas as pd
import urllib3

from dsi.backends.webserver import Webserver

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ----------------------------------------------------------------------
# Value Object (used for search results)
# ----------------------------------------------------------------------
class ValueObject:
    """Container for search results returned by find* methods."""

    def __init__(self):
        self.t_name = ""
        self.c_name = []
        self.row_num = None
        self.value = None
        self.type = ""


# ----------------------------------------------------------------------
# Oceans11 Backend (Webserver - read only)
# ----------------------------------------------------------------------
class Oceans11(Webserver):
    """DSI-based web backend for querying Oceans11 metadata in memory."""

    read_only = True

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize the backend and optionally load data from Oceans11.

        Parameters
        ----------
        url : str, optional
            Base Oceans11 catalog URL.
        params : dict or list[dict], optional
            Initial Tier-1 search parameters. Supported search keys include
            q, keyword, osti_id, title, author/authors, subject/subjects,
            doi, report_number, rows, and download_all.
        workspace : str, optional
            Directory where downloaded catalog/Tier-2 files are stored.
        only_validate : bool, optional
            Used by backend discovery. If True, validate local constructor
            configuration only and return without contacting Oceans11.
        """
        default_url = "https://oceans11.lanl.gov/dataCatalog/oceans11.db"
        base_url = url or default_url

        self.workspace = kwargs.get(
            "workspace",
            str(Path("./").expanduser()),
        )

        parsed = urlparse(base_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Oceans11 catalog URL must be http or https")

        self.base_url = base_url.rstrip("/")

        # Backend discovery/configuration check.  This mirrors the behavior
        # used by the other web backends and intentionally performs no I/O.
        if kwargs.get("only_validate", False):
            return

        # Tier 1 is stored as records. Tier 2 tables are loaded separately.
        self._cache = OrderedDict()
        self._tier1_tables = []
        self._resource_tables = []
        self._dataset_id_map = {}
        self._dataset_title_map = {}
        self._dataset_table_map = {}

        self._loaded = False
        self.catalog_path = None
        self.params = params or {}

        # Normal construction validates the live catalog by downloading it.
        try:
            self.catalog_path = self.validate_connection()
        except (ConnectionError, RuntimeError):
            self._loaded = False
            raise

        if self.params:
            try:
                self._load_initial_data(self.params)
                self._loaded = True
            except Exception as exc:
                self._loaded = False
                raise RuntimeError(f"Failed to load initial data: {exc}") from exc
        else:
            self._loaded = True

    # ------------------------------------------------------------------
    # Connection Validation
    # ------------------------------------------------------------------
    def validate_connection(self, **kwargs):
        """
        Validate that the Oceans11 catalog is accessible and usable.

        Normal use returns the downloaded local catalog path. If this method
        is called explicitly with ``only_validate=True``, it returns a bool
        and removes the temporary downloaded validation folder.
        """
        try:
            from contextlib import redirect_stdout
            import os
            import shutil

            from dsi.utils.federated.federate_datasets import pull_data

            with open(os.devnull, "w", encoding="utf-8") as fnull:
                with redirect_stdout(fnull):
                    info = pull_data(
                        location_type="url",
                        location=self.base_url,
                        path=self.base_url,
                        abs_path_workspace_folder=self.workspace,
                        username="",
                    )

            if info is None:
                if kwargs.get("only_validate", False):
                    return False
                raise ConnectionError(
                    f"Failed to download catalog from {self.base_url}"
                )

            local_path = info.get("local_path")
            if not local_path or not Path(local_path).is_file():
                if kwargs.get("only_validate", False):
                    return False
                raise RuntimeError("Downloaded catalog file is invalid or missing")

            if kwargs.get("only_validate", False):
                folder_hash = info.get("folder_hash")
                if folder_hash:
                    shutil.rmtree(folder_hash, ignore_errors=True)
                return True

            return local_path

        except Exception as exc:
            if kwargs.get("only_validate", False):
                return False
            raise ConnectionError(
                f"Unable to access Oceans11 catalog: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Initial Data Load
    # ------------------------------------------------------------------
    def _load_initial_data(self, params):
        """
        Fetch datasets/resources from the downloaded Oceans11 catalog.

        ``params`` may be a dict for one Tier-1 query or a list of dicts for
        multiple Tier-1 queries whose results are merged and deduplicated.
        """
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(
            isinstance(query, dict) for query in params
        ):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")

        from dsi.dsi import DSI

        self._catalog_dsi = DSI(
            filename=self.catalog_path,
            backend_name="Sqlite",
            silence_messages=True,
        )

        download_all = any(
            bool(query_params.get("download_all", False))
            for query_params in query_list
        )

        if download_all:
            unique_records = self._run_all_records_query()
        else:
            all_records = []

            for query_params in query_list:
                all_records.extend(
                    self._run_single_query(query_params)
                )

            unique_records = self._deduplicate_records(all_records)

        # ---------------------------------------------------
        # Tier 1
        # ---------------------------------------------------

        # Selected records are the primary Tier-1 table.
        self._cache["records"] = self._rows_to_table(unique_records)

        # Load only the filesystem rows associated with the
        # selected records. Preserve the actual catalog table name.
        filesystem_name, filesystem_data = (
            self._load_filesystem_table(unique_records)
        )

        self._cache[filesystem_name] = filesystem_data

        # ---------------------------------------------------
        # Tier 2
        # ---------------------------------------------------

        # Load Tier-2 databases in the same order that their
        # corresponding rows appear in records.
        for record in unique_records:
            t2db_url = record.get("t2db_url")

            if not t2db_url:
                continue

            t2db_path = self._download_t2_db(t2db_url)

            record["t2db_path"] = t2db_path
            record["t2db_name"] = self._derive_t2_db_name(
                record,
                t2db_path,
            )

            self._load_t2_tables(
                record,
                t2db_path,
            )

        # Rebuild records so the T2 path/name information added above
        # is included. Reassigning an existing OrderedDict key does
        # not move it, so records remains first.
        self._cache["records"] = self._rows_to_table(unique_records)

        self._dataset_id_map = {
            str(row.get("osti_id")): row
            for row in unique_records
            if row.get("osti_id") is not None
        }

        self._dataset_title_map = {
            str(row.get("title")): row
            for row in unique_records
            if row.get("title") is not None
        }

        self._loaded = True



    # ------------------------------------------------------------------
    # Data Load Helpers - Tier 1
    # ------------------------------------------------------------------
    def _load_filesystem_table(self, records):
        """Load filesystem rows associated with selected Oceans11 records."""

        tables = self._catalog_dsi.query(
            "SELECT name FROM sqlite_master WHERE type='table'",
            collection=True,
        )

        if isinstance(tables, pd.DataFrame):
            table_names = tables["name"].tolist()
        else:
            table_names = tables["name"]

        # Find the real table name without renaming it.
        fs_table = next(
            (
                name
                for name in table_names
                if re.sub(r"[^a-z0-9]", "", str(name).lower()) == "filesystem"
            ),
            None,
        )

        if fs_table is None:
            raise RuntimeError(
                f"Could not find filesystem table. "
                f"Available tables: {table_names}"
            )

        safe_table = fs_table.replace('"', '""')

        df = self._catalog_dsi.query(
            f'SELECT * FROM "{safe_table}"',
            collection=True,
        )

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        # T2 database filenames associated with selected records.
        selected_db_names = set()

        for record in records:
            t2db_url = record.get("t2db_url")

            if not t2db_url:
                continue

            db_name = Path(urlparse(t2db_url).path).name

            if db_name:
                selected_db_names.add(db_name.lower())

        # filesystem.file_origin maps to names such as "cloverleaf.db".
        if "file_origin" in df.columns:
            df = df[
                df["file_origin"]
                .astype(str)
                .str.lower()
                .isin(selected_db_names)
            ]

        return fs_table, self._rows_to_table(
            df.to_dict(orient="records")
        )


    def _run_all_records_query(self):
        """Return every record from the local Oceans11 Tier-1 catalog DB."""
        df = self._catalog_dsi.query("SELECT * FROM records", collection=True)
        if df is None or df.empty:
            return []
        return df.to_dict(orient="records")

    def _run_single_query(self, params):
        """Run one query against the local Oceans11 Tier-1 catalog DB."""
        supported_params = {
            "q",
            "keyword",
            "osti_id",
            "title",
            "author",
            "authors",
            "subject",
            "subjects",
            "doi",
            "report_number",
            "rows",
            "download_all",
        }

        unknown = set(params) - supported_params
        if unknown:
            raise ValueError(
                f"Unsupported Oceans11 search parameter(s): {sorted(unknown)}"
            )

        rows = int(params.get("rows", 20))
        if rows < 1:
            raise ValueError("rows must be greater than zero")

        clauses = []

        keyword_search_fields = [
            "title",
            "authors",
            "subjects",
            "description",
            "doi",
            "report_number",
        ]

        q = params.get("q") or params.get("keyword")
        if q:
            clauses.append(self._or_like_clause(q, keyword_search_fields))

        for field in ("osti_id", "doi", "report_number"):
            value = params.get(field)
            if value is not None:
                clauses.append(f"{field} = '{self._escape_sql(value)}'")

        # Partial-match fields. Singular names are aliases for consistency
        # with OSTI's public request vocabulary.
        field_aliases = (
            ("title", "title"),
            ("authors", "authors"),
            ("author", "authors"),
            ("subjects", "subjects"),
            ("subject", "subjects"),
        )
        for param_name, column_name in field_aliases:
            value = params.get(param_name)
            if value is not None:
                clauses.append(
                    f"{column_name} LIKE '%{self._escape_sql(value)}%'"
                )

        query = "SELECT * FROM records"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += f" LIMIT {rows}"

        df = self._catalog_dsi.query(query, collection=True)
        if df is None or df.empty:
            return []
        return df.to_dict(orient="records")

    def _deduplicate_records(self, records):
        """Deduplicate Tier-1 records using OSTI ID, DOI, or title."""
        seen = set()
        unique_records = []

        for record in records:
            if not isinstance(record, dict):
                continue

            key = (
                record.get("osti_id")
                or record.get("doi")
                or record.get("title")
            )

            if key is None:
                unique_records.append(record)
                continue

            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        return unique_records

    def _or_like_clause(self, value, fields):
        value = self._escape_sql(value)
        parts = [f"{field} LIKE '%{value}%'" for field in fields]
        return "(" + " OR ".join(parts) + ")"

    def _escape_sql(self, value):
        return str(value).replace("'", "''")

    # ------------------------------------------------------------------
    # Data Load Helpers - Tier 2
    # ------------------------------------------------------------------
    def _download_t2_db(self, t2db_url):
        """Download one Tier-2 database selected by a Tier-1 record."""
        from dsi.utils.federated.federate_datasets import pull_data

        base_url = "https://oceans11.lanl.gov/dataCatalog/"
        full_url = urljoin(base_url, t2db_url)

        info = pull_data(
            location_type="url",
            location=full_url,
            path=full_url,
            abs_path_workspace_folder=self.workspace,
            username="",
        )

        if info is None or not info.get("local_path"):
            raise RuntimeError(f"Failed to download T2 DB: {full_url}")

        return info["local_path"]

    def _derive_t2_db_name(self, record, t2db_path):
        """Return a stable, human-readable prefix for a Tier-2 database.

        Oceans11 downloads can be stored locally under an OSTI-ID filename
        such as ``2571471.db`` even when the remote catalog path identifies
        the database as ``heat``. Prefer the meaningful remote path component
        and only fall back to the dataset title when the URL is numeric-only.
        """
        t2db_url = str(record.get("t2db_url") or "")
        url_path = Path(urlparse(t2db_url).path)

        stem = url_path.stem if url_path.name else ""
        osti_id = str(record.get("osti_id") or "")

        def meaningful(value):
            value = str(value or "").strip()
            if not value or value.isdigit() or value == osti_id:
                return False
            return value.lower() not in {
                "datacatalog", "data", "db", "database", "databases",
                "download", "downloads"
            }

        # A nonnumeric remote filename is the most literal database name.
        if meaningful(stem):
            candidate = stem
        else:
            # Numeric files are commonly stored beneath a meaningful dataset
            # directory, e.g. ``heat/2571471.db`` -> ``heat``.
            parent = url_path.parent.name
            if meaningful(parent):
                candidate = parent
            else:
                # Final fallback: use the Tier-1 dataset title rather than an
                # opaque numeric local filename.
                candidate = str(record.get("title") or Path(t2db_path).stem)

        candidate = re.sub(r"[^0-9A-Za-z]+", "_", candidate).strip("_")
        return candidate.lower() or Path(t2db_path).stem.lower()

    def _load_t2_tables(self, record, t2db_path):
        """
        Load one downloaded Tier-2 database into the in-memory cache.

        Tier-2 table names are prefixed with a human-readable database name
        derived from the Tier-1 ``t2db_url``. Table names are normalized before
        being added to the cache so they can be safely processed into SQLite
        later.
        """
        from dsi.dsi import DSI

        t2_dsi = DSI(
            filename=t2db_path,
            backend_name="Sqlite",
            silence_messages=True,
        )

        db_name = self._derive_t2_db_name(record, t2db_path)
        record["t2db_name"] = db_name

        table_names = t2_dsi.list(collection=True)
        loaded_tables = []

        for table_name in table_names:
            # Keep the original SQLite-safe table name when reading the
            # source Tier-2 database.
            df = t2_dsi.get_table(table_name, collection=True)

            if df is None or df.empty:
                continue

            # Sqlite.list() may return quoted identifiers for reserved words,
            # for example '"view"'. Remove those outer quotes before creating
            # the Oceans11 cached table name.
            clean_table_name = str(table_name)

            if (
                len(clean_table_name) >= 2
                and clean_table_name.startswith('"')
                and clean_table_name.endswith('"')
            ):
                clean_table_name = clean_table_name[1:-1]

            # Normalize any remaining characters that could produce an invalid
            # identifier when this cache is later processed into SQLite.
            clean_table_name = re.sub(
                r"[^0-9A-Za-z_]+",
                "_",
                clean_table_name,
            ).strip("_")

            if not clean_table_name:
                continue

            cache_table_name = f"{db_name}_{clean_table_name}"

            self._cache[cache_table_name] = self._rows_to_table(
                df.to_dict(orient="records")
            )

            if cache_table_name not in self._resource_tables:
                self._resource_tables.append(cache_table_name)

            loaded_tables.append(cache_table_name)

        # Explicitly link each Tier-1 identifier to the tables loaded from
        # its downloaded Tier-2 database.
        for identifier in (
            record.get("osti_id"),
            record.get("title"),
            db_name,
        ):
            if identifier is not None:
                self._dataset_table_map[str(identifier)] = list(loaded_tables)

        return loaded_tables

    # ------------------------------------------------------------------
    # Table Name Resolution
    # ------------------------------------------------------------------
    def _resolve_table_name(self, identifier):
        """Resolve a cached table name or an unambiguous dataset identifier."""
        identifier_str = str(identifier)

        if identifier_str in self._cache:
            return identifier_str

        matches = list(self._dataset_table_map.get(identifier_str, []))

        if not matches and identifier_str in self._dataset_id_map:
            record = self._dataset_id_map[identifier_str]
            db_name = record.get("t2db_name")
            if db_name:
                matches = list(self._dataset_table_map.get(str(db_name), []))

        if not matches and identifier_str in self._dataset_title_map:
            record = self._dataset_title_map[identifier_str]
            db_name = record.get("t2db_name")
            if db_name:
                matches = list(self._dataset_table_map.get(str(db_name), []))

        if not matches:
            prefix = f"{identifier_str}_"
            matches = [
                table_name
                for table_name in self._resource_tables
                if table_name.startswith(prefix)
            ]

        if len(matches) == 1:
            return matches[0]

        if len(matches) > 1:
            raise ValueError(
                f"Dataset '{identifier}' contains multiple cached tables: {matches}. "
                "Specify the exact table name."
            )

        raise ValueError(
            f"Table '{identifier}' not found. "
            f"Available tables: {self._ordered_table_names()}"
        )

    # ------------------------------------------------------------------
    # Table Helpers
    # ------------------------------------------------------------------
    def _extract_tables(self, datasets):
        """
        Normalize selected Tier-1 rows and construct dataset lookup maps.

        Tier-2 tables are loaded separately from each selected row's
        ``t2db_path``.
        """
        dataset_rows = []
        resource_map = {}
        id_map = {}

        for dataset in datasets:
            dataset_id = dataset.get("osti_id") or dataset.get("doi")
            dataset_title = dataset.get("title") or dataset_id

            if dataset_id and dataset_title:
                id_map[dataset_id] = dataset_title

            row = {
                "osti_id": dataset.get("osti_id"),
                "title": dataset.get("title"),
                "authors": dataset.get("authors"),
                "subjects": dataset.get("subjects"),
                "description": dataset.get("description"),
                "doi": dataset.get("doi"),
                "report_number": dataset.get("report_number"),
                "t2db_url": dataset.get("t2db_url"),
                "t2db_path": dataset.get("t2db_path"),
                "t2db_name": dataset.get("t2db_name"),
            }

            for key, value in dataset.items():
                if key not in row:
                    row[key] = value

            dataset_rows.append(row)

            if dataset_title:
                resource_map.setdefault(dataset_title, [])

        return dataset_rows, resource_map, id_map

    def _rows_to_table(self, rows):
        """Convert list-of-dicts to a column-oriented OrderedDict."""
        if not rows:
            return OrderedDict()

        # Preserve first-seen order while retaining keys introduced by later rows.
        columns = list(rows[0].keys())
        for row in rows[1:]:
            for key in row.keys():
                if key not in columns:
                    columns.append(key)

        table = OrderedDict((column, []) for column in columns)
        for row in rows:
            for column in columns:
                table[column].append(row.get(column))

        return table

    # ------------------------------------------------------------------
    # Terminal / Backend Interface
    # ------------------------------------------------------------------
    def num_tables(self):
        """Print the number of cached Oceans11 tables."""
        table_count = len(self._cache)
        if table_count != 1:
            print(f"Database now has {table_count} tables")
        else:
            print(f"Database now has {table_count} table")

    def get_table(self, table_name="records", dict_return=False):
        """Return all data from a cached Oceans11 table."""
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot retrieve an empty backend.")

        resolved_name = self._resolve_table_name(table_name)
        table = self._cache.get(resolved_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found")

        if dict_return:
            return table
        return pd.DataFrame(table)

    def get_schema(self):
        """Return a lightweight CREATE TABLE-style schema for cached tables."""
        schema_lines = []

        for table_name, table in self._cache.items():
            columns = []
            for column_name, values in table.items():
                dtype = "TEXT"
                for value in values:
                    if value is None:
                        continue
                    if isinstance(value, bool):
                        dtype = "BOOLEAN"
                    elif isinstance(value, int):
                        dtype = "INTEGER"
                    elif isinstance(value, float):
                        dtype = "REAL"
                    elif isinstance(value, (dict, list, tuple, set)):
                        dtype = "OBJECT"
                    else:
                        dtype = "TEXT"
                    break

                columns.append(f"    {column_name} {dtype}")

            schema_lines.append(
                f"CREATE TABLE {table_name} (\n"
                + ",\n".join(columns)
                + "\n);"
            )

        return "\n\n".join(schema_lines)

    def get_table_names(self, query):
        """SQL table-name extraction is not supported for Oceans11."""
        raise NotImplementedError(
            "Oceans11 backend has not implemented get_table_names"
        )

    # ------------------------------------------------------------------
    # Query Interface
    # ------------------------------------------------------------------
    def query_artifacts(self, query, dict_return=True, **kwargs):
        """
        Public query() is not supported for Oceans11 because it is non-SQL.

        The private Tier-1 catalog search remains SQL-backed internally, but
        users should use find(), search(), find_relation(), or get_table().
        """
        raise NotImplementedError(
            "query() is not supported for Oceans11 backend - it is a non-SQL backend.\n\n"
            "Use these methods instead:\n"
            "  dsi.find(...)             # Search for values\n"
            "  dsi.search(...)           # Search across backend data\n"
            "  dsi.get_table(...)        # Retrieve a cached table\n"
        )

    # ------------------------------------------------------------------
    # Artifact Processing
    # ------------------------------------------------------------------
    def process_artifacts(self):
        """
        Return all cached Oceans11 data for export/process.

        Includes Tier-1 catalog tables and loaded Tier-2 tables.
        """
        if not self._loaded:
            return OrderedDict()

        return self._cache

    # ------------------------------------------------------------------
    # Find Methods
    # ------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """Search for a value across table, column, and cell levels."""
        if not self._loaded:
            return []

        query_str = str(query_object).lower()
        return (
            self.find_table(query_str)
            + self.find_column(query_str)
            + self.find_cell(query_object)
        )

    def find_table(self, query_object, **kwargs):
        """Find cached tables whose names contain ``query_object``."""
        if not self._loaded or not isinstance(query_object, str):
            return []

        matches = []
        for table_name, table_data in self._cache.items():
            if query_object.lower() in table_name.lower():
                value = ValueObject()
                value.t_name = table_name
                value.c_name = list(table_data.keys())
                value.value = table_data
                value.type = "table"
                matches.append(value)

        return matches

    def find_column(self, query_object, **kwargs):
        """Find cached columns whose names contain ``query_object``."""
        if not self._loaded or not isinstance(query_object, str):
            return []

        matches = []
        for table_name, table_data in self._cache.items():
            for column_name, column_data in table_data.items():
                if query_object.lower() in column_name.lower():
                    value = ValueObject()
                    value.t_name = table_name
                    value.c_name = [column_name]
                    value.value = column_data
                    value.type = "column"
                    matches.append(value)

        return matches

    def find_cell(self, query_object, row=False, **kwargs):
        """
        Find matching cells across all cached tables.

        When row=False, return one ValueObject per matching cell.
        When row=True, return each matching row only once.
        """
        if not self._loaded:
            return []

        matches = []
        is_string_query = isinstance(query_object, str)
        query_lower = query_object.lower() if is_string_query else None

        for table_name, table_data in self._cache.items():
            if not table_data:
                continue

            columns = list(table_data.keys())
            rows = zip(*table_data.values())

            for row_idx, row_data in enumerate(rows):
                for column_idx, cell in enumerate(row_data):
                    match = query_object == cell

                    if (
                        not match
                        and is_string_query
                        and isinstance(cell, str)
                        and query_lower in cell.lower()
                    ):
                        match = True
                    elif (
                        not match
                        and is_string_query
                        and isinstance(cell, (dict, list, tuple))
                        and query_lower in str(cell).lower()
                    ):
                        match = True

                    if not match:
                        continue

                    value = ValueObject()
                    value.t_name = table_name
                    value.row_num = row_idx

                    if row:
                        value.c_name = columns
                        value.value = list(row_data)
                        value.type = "row"
                        matches.append(value)

                        # search() wants matching ROWS, not one copy
                        # of the row for every matching cell.
                        break

                    value.c_name = [columns[column_idx]]
                    value.value = cell
                    value.type = "cell"
                    matches.append(value)

        return matches

    def find_relation(self, column_name, relation, **kwargs):
        """
        Find rows in cached tables satisfying a relation.

        Searches every loaded table containing ``column_name``.
        """
        if not self._loaded:
            raise RuntimeError(
                "find_relation() ERROR: Cannot search an empty backend."
            )

        # Find every loaded table containing this column.
        matching_tables = [
            table_name
            for table_name, table_data in self._cache.items()
            if column_name in table_data
        ]

        if not matching_tables:
            return (
                f"'{column_name}' is not a column in this database. "
                "Ensure the column is written first."
            )

        if len(matching_tables) > 1:
            return [
                f"SELECT * FROM {table_name} WHERE {column_name} {relation}"
                for table_name in matching_tables
            ]

        relation_operator, value = self._parse_relation(relation)

        matches = []

        for table_name in matching_tables:
            df = pd.DataFrame(self._cache[table_name])

            filtered = self._apply_pandas_filter(
                df,
                column_name,
                relation_operator,
                value,
            )

            for idx, row_data in filtered.iterrows():
                value_object = ValueObject()
                value_object.t_name = table_name
                value_object.c_name = list(df.columns)
                value_object.row_num = int(idx) + 1
                value_object.value = row_data.tolist()
                value_object.type = "relation"
                matches.append(value_object)

        if not matches:
            return (
                f"Could not find any rows where "
                f" {column_name} {relation}  in this database."
            )

        return matches 

    def _apply_pandas_filter(self, df, column, relation_operator, value):
        """Apply a parsed DSI relation to a DataFrame column."""
        series = df[column]

        if relation_operator == "contains":
            mask = series.astype(str).str.contains(
                str(value),
                case=False,
                na=False,
            )
            return df[mask]

        if relation_operator == "range":
            min_value, max_value = value
            if (
                isinstance(min_value, (int, float))
                and not isinstance(min_value, bool)
                and isinstance(max_value, (int, float))
                and not isinstance(max_value, bool)
            ):
                numeric = pd.to_numeric(series, errors="coerce")
                mask = numeric.between(min_value, max_value)
            else:
                text = series.astype("string")
                mask = (
                    (text >= str(min_value))
                    & (text <= str(max_value))
                )
            return df[mask.fillna(False)]

        if value is None:
            if relation_operator == "==":
                return df[series.isna()]
            if relation_operator == "!=":
                return df[series.notna()]

        if isinstance(value, bool):
            if relation_operator == "==":
                return df[series == value]
            if relation_operator == "!=":
                return df[series != value]

        if relation_operator in {"==", "!="}:
            if pd.api.types.is_numeric_dtype(series):
                compare_value = value
                compare_series = series
            else:
                compare_value = str(value)
                compare_series = series.astype("string")

            if relation_operator == "==":
                mask = compare_series == compare_value
            else:
                mask = compare_series != compare_value

            return df[mask.fillna(False)]

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            compare_series = pd.to_numeric(series, errors="coerce")
        else:
            compare_series = series.astype("string")
            value = str(value)

        if relation_operator == ">":
            mask = compare_series > value
        elif relation_operator == "<":
            mask = compare_series < value
        elif relation_operator == ">=":
            mask = compare_series >= value
        elif relation_operator == "<=":
            mask = compare_series <= value
        else:
            raise ValueError(
                f"Unsupported relation operator: {relation_operator}"
            )

        return df[mask.fillna(False)]

    def _parse_relation(self, relation):
        """Parse a DSI relation into an operator and Python value."""
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
                return (
                    "range",
                    (
                        self._parse_value(values[0]),
                        self._parse_value(values[1]),
                    ),
                )

        raise ValueError(f"Unknown relation format: {relation}")

    def _parse_value(self, value):
        """Convert a relation value into an appropriate Python value."""
        value = str(value).strip()

        if (
            (value.startswith("'") and value.endswith("'"))
            or (value.startswith('"') and value.endswith('"'))
        ):
            value = value[1:-1]

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

    # ------------------------------------------------------------------
    # Utility / Display
    # ------------------------------------------------------------------
    def _ordered_table_names(self):
        """Return cached tables in their load order."""
        return list(self._cache.keys())

    def list(self, collection=False):
        """List loaded Oceans11 tables."""

        if not self._loaded:
            return [] if collection else None

        table_names = list(self._cache.keys())

        if collection:
            return table_names

        for table_name in table_names:
            table = self._cache[table_name]

            num_columns = len(table)

            if table:
                first_column = next(iter(table.values()))
                num_rows = len(first_column)
            else:
                num_rows = 0

            print(f"\nTable: {table_name}")
            print(f"  - num of columns: {num_columns}")
            print(f"  - num of rows: {num_rows}")

    def summary(self, table_name=None):
        """
        Return column-level summary metadata for cached Oceans11 tables.

        With no table name, returns
        ``[table_name_list, table1_df, table2_df, ...]``.
        """
        if table_name is not None:
            resolved_name = self._resolve_table_name(table_name)
            if resolved_name not in self._cache:
                raise ValueError(
                    f"Table '{table_name}' not found. "
                    f"Available tables: {list(self._cache.keys())}"
                )
            if not self._cache[resolved_name]:
                raise ValueError(f"Table '{resolved_name}' is empty")

            return self._summary_helper(resolved_name)

        table_names = []
        summary_dfs = []

        for name in self._ordered_table_names():
            table = self._cache[name]
            if not table:
                continue
            table_names.append(name)
            summary_dfs.append(self._summary_helper(name))

        return [table_names] + summary_dfs

    def _summary_helper(self, table_name):
        """Generate OSTI-style column-level summary metadata."""
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
            "t2db_url",
            "t2db_path",
            "t2db_name",
        }

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
            elif non_null.empty or all(
                isinstance(value, str) for value in non_null
            ):
                column_type = "TEXT"
            else:
                column_type = "OBJECT"

            has_complex_values = (
                non_null.apply(
                    lambda item: isinstance(item, (dict, list, tuple, set))
                ).any()
                if not non_null.empty
                else False
            )

            if has_complex_values:
                unique_values = int(non_null.astype(str).nunique())
            else:
                unique_values = int(non_null.nunique())

            min_value = None
            max_value = None
            average = None
            std_dev = None

            numeric_series = pd.to_numeric(
                non_null,
                errors="coerce",
            ).dropna()

            if not non_null.empty and len(numeric_series) == len(non_null):
                min_value = numeric_series.min()
                max_value = numeric_series.max()
                average = numeric_series.mean()
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
                    min_value = non_null.min()
                    max_value = non_null.max()
                except TypeError:
                    pass

            rows.append(
                [
                    column,
                    column_type,
                    unique_values,
                    min_value,
                    max_value,
                    average,
                    std_dev,
                ]
            )

        return pd.DataFrame(rows, columns=headers, dtype=object)

    def display(self, table_name="records", num_rows=25, display_cols=None):
        """Return rows from a cached Oceans11 table as a DataFrame."""
        if not self._loaded:
            raise RuntimeError("No data loaded. Cannot display empty backend.")

        resolved_name = self._resolve_table_name(table_name)
        table = self._cache.get(resolved_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found")

        df = pd.DataFrame(table)
        if df.empty:
            raise ValueError(f"The '{resolved_name}' table is empty")

        if display_cols:
            missing_columns = set(display_cols) - set(df.columns)
            if missing_columns:
                raise ValueError(
                    f"Columns not found in '{resolved_name}': {missing_columns}\n"
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        df.attrs["max_rows"] = len(df)
        if num_rows:
            df = df.head(num_rows)

        return df.map(
            lambda value: (
                str(value)[:60] + "..."
                if isinstance(value, str) and len(value) > 60
                else value
            )
        )

    def notebook(self, **kwargs):
        """Notebook generation is not supported for Oceans11."""
        pass

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self):
        """Close the backend and clear all cached state."""
        self._cache.clear()
        self._tier1_tables.clear()
        self._resource_tables.clear()
        self._dataset_id_map.clear()
        self._dataset_title_map.clear()
        self._dataset_table_map.clear()

        if hasattr(self, "_catalog_dsi"):
            try:
                self._catalog_dsi.close()
            except Exception:
                pass

        self.catalog_path = None
        self._loaded = False

    # ------------------------------------------------------------------
    # Abstract Methods
    # ------------------------------------------------------------------
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """Oceans11 is read-only."""
        raise NotImplementedError("Oceans11 backend is read-only")
