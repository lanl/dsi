"""
Denodo Data Catalog Backend for DSI

Read-only backend that pulls metadata from the Denodo Data Catalog
REST API and exposes it as in-memory DSI tables:
denodo_databases, denodo_views, denodo_columns, denodo_properties.
"""

import os
import webbrowser
import json
from pathlib import Path

from collections import OrderedDict
from urllib.parse import urlparse, parse_qs, urlencode
from typing import ClassVar  

import numpy as np
import pandas as pd
import requests

from dsi.backends.webserver import Webserver

try:
    import truststore
    truststore.inject_into_ssl()  # trust LANL internal TLS certificates
except ImportError:
    pass

# ----------------------------------------------------------------------
# Local configuration file (~/.denodo/config.json)
#
# Per DSI team convention: site-specific values (base URL, OAuth client
# settings) live in a per-user config file in the home directory --
# never in this public source file. Precedence for every setting:
#   1. explicit argument  >  2. environment variable  >  3. config file
# ----------------------------------------------------------------------
CONFIG_DIR = Path.home() / ".denodo"
CONFIG_PATH = CONFIG_DIR / "config.json"


def _load_config():
    """Read the optional local config file. Returns {} if absent."""
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {CONFIG_PATH}: {e}") from e


def _setting(name, env_var=None, config=None, default=None):
    """Resolve one setting: environment variable first, then config file."""
    if env_var:
        value = os.environ.get(env_var)
        if value:
            return value
    if config is None:
        config = _load_config()
    return config.get(name, default)


def save_config(**settings):
    """
    Create or update ~/.denodo/config.json (one-time setup helper).

    Example
    -------
    >>> from dsi.backends.denodo import save_config
    >>> save_config(base_url="https://<data-catalog-host>",
    ...             client_id="...", client_secret="...",
    ...             auth_url="...", token_url="...",
    ...             redirect_uri="...", scope="...")
    """
    CONFIG_DIR.mkdir(exist_ok=True)
    config = _load_config()
    config.update({k: v for k, v in settings.items() if v is not None})
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    print(f"Saved {len(settings)} setting(s) to {CONFIG_PATH}")


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
# OAuth helper (module-level)
# ----------------------------------------------------------------------
def get_token():
    """
    Run the OAuth 2.0 authorization-code flow and return a Bearer token.

    Reads the OAuth client configuration from environment variables
    first, then falls back to the local config file
    ``~/.denodo/config.json`` (see README "Configuration"):

        ==================  =========================
        Environment var     Config file key
        ==================  =========================
        AUTH_URL            auth_url
        TOKEN_URL           token_url
        AUTH_FLOW_CLIENT_ID client_id
        AUTH_FLOW_CLIENT_SECRET  client_secret
        REDIRECT_URI        redirect_uri
        SCOPE               scope
        ==================  =========================

    Opens the system browser for the login, then exchanges the
    returned authorization code for an access token.

    Returns
    -------
    str
        OAuth access token, used as the Bearer token by the backend.

    Raises
    ------
    ValueError
        If any required setting is missing from both sources, or the
        pasted redirect URL contains no authorization code.
    """

    config = _load_config()

    auth_url = _setting("auth_url", "AUTH_URL", config)
    token_url = _setting("token_url", "TOKEN_URL", config)
    client_id = _setting("client_id", "AUTH_FLOW_CLIENT_ID", config)
    client_secret = _setting("client_secret", "AUTH_FLOW_CLIENT_SECRET", config)
    redirect_uri = _setting("redirect_uri", "REDIRECT_URI", config)
    scope = _setting("scope", "SCOPE", config)

    missing = [name for name, value in [
        ("auth_url", auth_url), ("token_url", token_url),
        ("client_id", client_id), ("client_secret", client_secret),
        ("redirect_uri", redirect_uri), ("scope", scope),
    ] if not value]
    if missing:
        raise ValueError(
            "Missing OAuth configuration: " + ", ".join(missing) +
            f". Set them in {CONFIG_PATH} (see save_config) or as "
            "environment variables. See the README Configuration section."
        )

    # Step 1: open the browser at the login page
    query = urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
    })
    print("Opening browser for login...")
    webbrowser.open(f"{auth_url}?{query}")

    # Step 2: user pastes back the redirect URL after logging in
    redirected = input("After logging in, paste the full redirect URL here: ").strip()
    code = parse_qs(urlparse(redirected).query).get("code", [None])[0]
    if not code:
        raise ValueError("No authorization code found in the pasted URL.")

    # Step 3: exchange the authorization code for an access token
    resp = requests.post(
        token_url,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# ----------------------------------------------------------------------
# Denodo Backend (Webserver - Read only)
# ----------------------------------------------------------------------
class Denodo(Webserver):
    """
    Denodo Data Catalog web backend for querying view metadata in-memory
    """
    read_only = True

    # One row per hit from POST /search/metadata (no GET calls).
    # Column order here = column order in the DSI table.
    SEARCH_SCHEMA: ClassVar[list[str]] = [
        "name",
        "database_name",               # from database.databaseName
        "id",
        "database_id",                 # from database.databaseId
        "description",
        "descriptionType",
        "categories",                  # category names, comma-joined
        "tags",                        # tag names, comma-joined
        "lastModificationVdpData",
        "lastModificationIsstData",
        "matchedFields",
        "matchedCustomProperties",
        "matchedFieldsTagged",
        "countEndorsements",
        "countWarnings",
        "countDeprecations",
        "ranking",
    ]

    SUPPORTED_PARAMS: ClassVar[set[str]] = {
        "keywords",      # text to search ("" = whole catalog)
        "search_in",     # name | description | properties | column_names | column_descriptions
        "match",         # substring | all_words | any_words
        "categories",    # category ids
        "tags",          # tag ids
        "limit",         # max rows (default: every hit)
        "database",      # existing path: all views of one database
        "view",          # existing path: one view (needs 'database')
    }

    # Friendly param values -> Denodo API enums (confirmed via Swagger + probes)
    SEARCH_SCOPES: ClassVar[dict[str, str]] = {
        "name": "ELEMENT_NAME",
        "description": "ELEMENT_DESC",
        "properties": "PROPERTY_VALUE",
        "column_names": "FIELD_NAME",
        "column_descriptions": "FIELD_DESC",
    }

    MATCH_TYPES: ClassVar[dict[str, str]] = {
        "substring": "EXACT_MATCH",  # whole string as case-insensitive substring
        "all_words": "ALL_WORDS",    # AND
        "any_words": "ANY_WORDS",    # OR
    }

    # Defaults follow Divya's directive: name + description, ANY_WORDS
    DEFAULT_SEARCH_IN: ClassVar[list[str]] = ["name", "description"]
    DEFAULT_MATCH: ClassVar[str] = "any_words"

   # ----------------------------------------------------------------------
    # Class-level constants (next to SEARCH_SCHEMA / SUPPORTED_PARAMS)
    # ----------------------------------------------------------------------
    API_PATH: ClassVar[str] = "/denodo-data-catalog/public/api"


    # ----------------------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------------------
    def __init__(self, url=None, params=None, **kwargs):
        """
        Initialize backend and optionally load data from the Denodo
        Data Catalog REST API.

        Parameters
        ----------
        `url` : str, optional
            Base Data Catalog URL. If None, resolved from the
            DENODO_BASE_URL environment variable, then from the
            local config file ~/.denodo/config.json.
        `params` : dict, optional
            Dictionary of initial query parameters used to fetch data.

            Supported keys:
                - keywords : str - Full-text metadata search
                - database : str - Load all views from one database
                - view : str - Direct view lookup (requires 'database')
                - limit : int - Maximum number of views to retrieve
                  (default: 100)
        `**kwargs` : dict
            Additional keyword arguments:
                - token : str, optional
                    OAuth Bearer token. If not provided, the OAuth
                    authorization-code flow runs automatically
                    (see get_token()).
                - server_id : int, optional
                    Denodo server id (default 1)
                - verify_ssl : bool, optional
                    Toggle SSL verification (default True)

        Note
        ----
        Authentication happens before initialization: a valid OAuth
        Bearer token is required before the backend can load any data.
        Tokens expire, so a new login may be needed each session.
        """


        # A probe (only_validate=True, no token) must never raise and must
        # never open the OAuth browser popup: dsi.list_backends() does exactly
        # that to ask "is this backend reachable?"
        only_validate = kwargs.get("only_validate", False)

        DEFAULT_URL = _setting("base_url", env_var="DENODO_BASE_URL")
        base_url = url or DEFAULT_URL

        # ----------------------------------------------------------------------
        # Auth / Connection Config
        # ----------------------------------------------------------------------
        # Cheap validation first: check the URL before running the
        # (expensive, interactive) OAuth flow.
        if not base_url:
            if only_validate:
                self.base_url = None
                self.token = None
                self.headers = {}
                self.server_id = kwargs.get("server_id", 1)
                self.verify_ssl = kwargs.get("verify_ssl", True)
                self.validate_error_msg = (
                    "No base URL configured. Run save_config(base_url=...) or "
                    "set the DENODO_BASE_URL environment variable."
                )
                self._auth_probe = True
                return
            raise ValueError(
                "No base URL provided. Pass url=..., set the "
                "DENODO_BASE_URL environment variable, or run "
                f"save_config(base_url=...) to store it in {CONFIG_PATH}."
            )

        parsed = urlparse(base_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid base_url")

        self.base_url = base_url.rstrip("/")

        # Authentication step runs before the rest of initialization:
        # if no token is supplied, run the OAuth flow now -- except for a
        # probe, which only asks whether the Data Catalog is reachable.
        self.token = kwargs.get("token")
        self._auth_probe = only_validate and not self.token
        if not self.token and not only_validate:
            self.token = get_token()
        self.server_id = kwargs.get("server_id", 1)
        self.verify_ssl = kwargs.get("verify_ssl", True)

        self.headers = {}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        # Table registry: table name -> its columns (the shape).
        # One table for now; the four contract tables are added here
        # as their schemas are defined.
        self.schemas = {
            "denodo_search_results": self.SEARCH_SCHEMA,
        }

        # Table data: table name -> column-oriented OrderedDict.
        # Every registered table exists from the start, even when empty.
        self._cache = OrderedDict(
            (name, self._rows_to_table([], columns))
            for name, columns in self.schemas.items()
        )
        self._view_map = {}

        self._loaded = False
        self.params = params or {}
        self.validate_error_msg = None

        # skip data retrieval if only checking connection
        if kwargs.get("only_validate", False):
            return


        
        # Validate connection before attempting to load data
        if not self.validate_connection():
            self._loaded = False
            raise ConnectionError(
                self.validate_error_msg or "Failed to connect to the Denodo Data Catalog"
            )

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
        Validates that the Data Catalog base URL is accessible and functional.

        This method tests the connection by making a simple API call to verify:
            - The URL is reachable
            - The Data Catalog REST API is responding
            - The OAuth token is accepted

        Returns
        -------
        bool
            True if connection is valid.
            False if connection is not valid.
        """

        try:
            if not self.base_url:
                return False          # probe with no configured URL

            
            test_url = f"{self.base_url}/denodo-data-catalog/public/api/home"

            response = requests.get(
                test_url,
                headers=self.headers,
                verify=self.verify_ssl,
                timeout=60,
            )

            response.raise_for_status()
            response.json()   # confirm the body is valid JSON

            return True
        
        # Need to silent exit to continue external workflows
        except requests.exceptions.Timeout:
            self.validate_error_msg = f"Connection timeout: Cannot reach {self.base_url} within 60 seconds"
            return False
        except requests.exceptions.ConnectionError:
            self.validate_error_msg = f"Connection failed: Cannot connect to {self.base_url}. Check your network connection."
            return False
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in (401, 403):
                if getattr(self, "_auth_probe", False):
                    # Reachable, but we deliberately sent no token: for a
                    # probe, "the Data Catalog answered" is the answer.
                    return True
                self.validate_error_msg = (
                    f"Authentication rejected by {self.base_url} "
                    f"(HTTP {e.response.status_code}). The OAuth token may be "
                    "expired or lack the required scope -- re-authenticate."
                )
                return False
            elif e.response.status_code == 404:
                self.validate_error_msg = f"Data Catalog API not found at {self.base_url}. Verify this is a valid Data Catalog endpoint."
                return False
            else:
                self.validate_error_msg = f"HTTP {e.response.status_code} Error: {str(e)}"
                return False
        except requests.exceptions.RequestException as e:
            self.validate_error_msg = f"Failed to validate connection to {self.base_url}: {str(e)}"
            return False
        except ValueError as e:
            self.validate_error_msg = f"Invalid JSON response from {self.base_url}: {str(e)}"
            return False
        except Exception:
            return False


    # ----------------------------------------------------------------------
    # Initial Data Load
    # ----------------------------------------------------------------------
    def _load_initial_data(self, params):
        """
        Loads metadata from the Data Catalog API based on query parameters.

        Supports:
            - Single query (dict)
            - Multiple queries (list of dicts)
            - Direct view lookup (view + database parameters)

        Results are deduplicated by (database, view) and stored across
        four DSI tables, organized by data layer:

            Layer 1 (database):  denodo_databases
                one row per VDB
            Layer 2 (view):      denodo_views + denodo_properties
                one row per view in denodo_views; the 341 custom
                properties are Layer-2 metadata stored in a separate
                long EAV table (denodo_properties) purely for storage
                reasons -- wide fixed columns would break whenever a
                new property is defined
            Layer 2.5 (schema):  denodo_columns
                one row per column of each view
            Layer 3 (resource):  no dedicated table in v1.0
                the only verified per-view resource is a scalar 1:1
                documentation URL, folded into denodo_views as the
                nullable documentation_url column

        Note: tables are storage units, layers are concepts -- the
        mapping is not 1:1 (Layer 2 spans two tables; Layer 3 lives
        inside Layer 2's table). Data layers are also orthogonal to
        this backend's four processing stages (transport -> wrappers ->
        normalization -> assembly): every layer flows through all four.

        Parameters
        ----------
        params : dict or list of dict
            Query parameters or list of query parameter dicts.
            Each dict can contain:
                - view : str - Direct view lookup (requires 'database')
                - database : str - Load all views from one database
                - keywords : str - Full-text metadata search
                - limit : int - Maximum number of views (default: 100)
        """
        # Normalize params to list
        if isinstance(params, dict):
            query_list = [params]
        elif isinstance(params, list) and all(isinstance(p, dict) for p in params):
            query_list = params
        else:
            raise TypeError("params must be a dict or a list of dicts")


        # Collect results from all queries
        all_views = []     # view/database paths -> full view-details dicts
        search_rows = []   # search path -> flat rows from the POST response only

        for query_params in query_list:
            # Check if this is a direct view lookup
            if "view" in query_params:
                if not query_params.get("database"):
                    raise ValueError(
                        "Direct 'view' lookup requires 'database' -- a Denodo "
                        "view is identified by database name + view name."
                    )
                view = self._get_view_details(
                    query_params["view"], query_params["database"]
                )
                if view:
                    all_views.append(view)
            elif "database" in query_params:
                # All views of one database
                all_views.extend(
                    self._get_database_views(
                        query_params["database"], query_params.get("limit", 100)
                    )
                )
            else:
                # Search: POST /search/metadata only, no view-details calls
                search_rows.extend(self._run_single_query(query_params))

        # Search results: one row per hit, deduplicated by (database, view)
        if search_rows:
            seen, unique_rows = set(), []
            for row in search_rows:
                key = (row.get("database_name"), row.get("name"))
                if key not in seen:
                    seen.add(key)
                    unique_rows.append(row)
            self._cache["denodo_search_results"] = self._rows_to_table(
                unique_rows, self.SEARCH_SCHEMA
            )

        # The four contract tables come only from the view/database paths.
        # Skipped until _extract_tables and their schemas exist (Phase 2).
        if all_views:
            # Deduplicate by (database, view)
            unique_views = self._deduplicate_views(all_views)

            # Extract the four contract tables from deduplicated views
            db_rows, view_rows, column_rows, property_rows, view_map = \
                self._extract_tables(unique_views)

            # Layer 1 (database): one row per VDB
            self._cache["denodo_databases"] = self._rows_to_table(db_rows)

            # Layer 2 (view): one row per view; the Layer-3 resource lives
            # here as the nullable documentation_url column (v1.0 decision)
            self._cache["denodo_views"] = self._rows_to_table(view_rows)

            # Layer 2.5 (schema): one row per column of each view
            self._cache["denodo_columns"] = self._rows_to_table(column_rows)

            # Layer 2 detail: 341 custom properties as a long EAV table
            # (separate table for storage reasons only -- still Layer-2
            # metadata). All four tables are always created, even when
            # empty -- per DATA_CONTRACT.
            self._cache["denodo_properties"] = self._rows_to_table(property_rows)

            self._view_map = view_map

        self._loaded = True




        # Layer 3 (attached resources) has no dedicated table in v1.0.
        # The Step 0 probe verified only one real per-view resource:
        # a documentation URL embedded in the description field
        # (544 URLs, strictly 1:1 with views, scalar value). A separate
        # table only pays off for 1:many relationships; for a 1:1
        # scalar it would just add a pointless JOIN. The resource is
        # therefore folded into denodo_views as the nullable
        # documentation_url column. If a future Denodo version exposes
        # 1:many attachments, reintroduce a denodo_resources table.


    def _get_view_details(self, view_name, db_name):
        """
        Retrieve a single view's full metadata using view-details.

        Parameters
        ----------
        view_name : str
            View name
        db_name : str
            Database the view belongs to

        Returns
        -------
        dict or None
            View dict if found, None otherwise
        """
        try:
            return self._request("view-details", {
                "viewName": view_name,
                "databaseName": db_name,
                "serverId": self.server_id,
            })
        except (requests.exceptions.RequestException, RuntimeError, ValueError) as e:
            print(f"Warning: Could not retrieve view '{db_name}.{view_name}': {e}")
            return None


    def _get_database_views(self, db_name, limit=100):
        """
        Load all views belonging to one database (up to limit).

        The views list endpoint has no verified server-side database
        filter, so the full list is fetched once and filtered
        client-side, then each match is enriched via view-details.

        Parameters
        ----------
        db_name : str
            Database name
        limit : int, default 100
            Maximum number of views to load

        Returns
        -------
        list of dict
            Full view dicts for the database's views.
        """
        result = self._request("views", params={"serverId": self.server_id})

        if isinstance(result, list):
            all_views = result
        else:
            all_views = result.get("views", result.get("elements", []))

        matches = [
            v for v in all_views
            if not v.get("deleted")
            and (v.get("databaseName") or v.get("db")) == db_name
        ]

        views = []
        for v in matches[:limit]:
            view = self._get_view_details(v.get("name"), db_name)
            if view:
                views.append(view)

        return views



    def _run_single_query(self, params):
        """
        Run one metadata search (POST /search/metadata) and return one
        flat row per hit.

        No view-details calls: every column of denodo_search_results
        comes from the search response itself (16 keys per hit,
        flattened to SEARCH_SCHEMA by _flatten_hit).

        Parameters
        ----------
        params : dict
            Supported keys:
                - keywords : str, default ""
                    Text to search ("" = whole catalog)
                - search_in : list of str, default DEFAULT_SEARCH_IN
                    Any of: name, description, properties,
                    column_names, column_descriptions
                    (-> whereToSearchList, see SEARCH_SCOPES)
                - match : str, default DEFAULT_MATCH
                    'substring' | 'all_words' | 'any_words'
                    (-> searchType, see MATCH_TYPES)
                - categories : list of int - category ids (default: no filter)
                - tags : list of int - tag ids (default: no filter)
                - limit : int, optional
                    Maximum number of rows (default: every hit)

        Returns
        -------
        list of OrderedDict
            One flattened row per search hit, keyed by SEARCH_SCHEMA.
        """
        search_in = params.get("search_in", self.DEFAULT_SEARCH_IN)
        if isinstance(search_in, str):
            search_in = [search_in]
        match = params.get("match", self.DEFAULT_MATCH)
        limit = params.get("limit")

        unknown = [s for s in search_in if s not in self.SEARCH_SCOPES]
        if unknown:
            raise ValueError(
                f"Unknown search_in value(s) {unknown}. "
                f"Valid: {list(self.SEARCH_SCOPES)}"
            )
        if match not in self.MATCH_TYPES:
            raise ValueError(
                f"Unknown match '{match}'. Valid: {list(self.MATCH_TYPES)}"
            )

        # Body mirrors MetadataSearchInput exactly; every starred field present.
        # The three id lists must be present even when empty (400 otherwise).
        # with* flags pinned False: they FILTER ("only elements having"), not include.
        page_size = 100
        body = {
            "text": params.get("keywords", ""),
            "elementType": "VIEWS",                      # required (starred)
            "withEndorsements": False,                   # required (starred)
            "withWarnings": False,                       # required (starred)
            "withDeprecations": False,
            "categoryIds": params.get("categories", []),
            "tagIds": params.get("tags", []),
            "databaseIds": [],
            "whereToSearchList": [self.SEARCH_SCOPES[s] for s in search_in],
            "searchType": self.MATCH_TYPES[match],
            "offset": 0,
            "limit": page_size,
        }

        # Paginate until every hit is collected (or limit is reached)
        hits = []
        while True:
            result = self._request(
                "search/metadata",
                params={"serverId": self.server_id},
                method="POST",
                json_body=body,
            )
            batch = result.get("elements", [])
            total = result.get("elementsCount", 0)
            hits.extend(batch)
            if not batch or len(hits) >= total or (limit and len(hits) >= limit):
                break
            body["offset"] += page_size

        # Self-check: a full harvest must match the server's own count
        if not limit and len(hits) != total:
            print(f"Warning: collected {len(hits)} hits, elementsCount says {total}")

        return [self._flatten_hit(hit) for hit in hits[:limit]]



    def _deduplicate_views(self, views):
        """
        Remove duplicate views based on the (database, view) identity pair.

        Parameters
        ----------
        views : list
            List of view dicts from the Data Catalog API

        Returns
        -------
        list
            Deduplicated list of views
        """
        seen_keys = set()
        unique_views = []

        for v in views:
            db = v.get("databaseName") or v.get("db")
            key = (db, v.get("name"))

            if key not in seen_keys:
                seen_keys.add(key)
                unique_views.append(v)

        return unique_views


    

    # ----------------------------------------------------------------------
    # Table Helpers
    # ----------------------------------------------------------------------
    def _rows_to_table(self, rows, schema):
        """
        Convert a list of row dicts into a column-oriented OrderedDict.

        Columns come from `schema`, not from the rows, so an empty
        result still produces every column (in schema order) and a
        key missing from a row becomes None.

        Parameters
        ----------
        rows : list of dict
            One dict per row.
        schema : list of str
            Column names, in table order (e.g. SEARCH_SCHEMA).

        Returns
        -------
        OrderedDict
            {column_name: [value_row0, value_row1, ...]}
        """
        return OrderedDict((col, [row.get(col) for row in rows]) for col in schema)


    @staticmethod
    def _flatten_hit(hit):
        """
        Flatten one POST /search/metadata hit into a SEARCH_SCHEMA row.

        A table cell holds one value, so the three nested keys are
        flattened: `database` (dict) splits into database_name and
        database_id; `categories` and `tags` (lists of {id, name})
        become comma-joined names, None when empty. The other 13 keys
        are copied as-is.

        Parameters
        ----------
        hit : dict
            One element of the search response's `elements` list.

        Returns
        -------
        OrderedDict
            One row, keys in SEARCH_SCHEMA order.
        """
        db = hit.get("database") or {}

        def names(items):
            return ", ".join(i.get("name", "") for i in items or []) or None

        return OrderedDict([
            ("name", hit.get("name")),
            ("database_name", db.get("databaseName")),
            ("id", hit.get("id")),
            ("database_id", db.get("databaseId")),
            ("description", hit.get("description")),
            ("descriptionType", hit.get("descriptionType")),
            ("categories", names(hit.get("categories"))),
            ("tags", names(hit.get("tags"))),
            ("lastModificationVdpData", hit.get("lastModificationVdpData")),
            ("lastModificationIsstData", hit.get("lastModificationIsstData")),
            ("matchedFields", hit.get("matchedFields")),
            ("matchedCustomProperties", hit.get("matchedCustomProperties")),
            ("matchedFieldsTagged", hit.get("matchedFieldsTagged")),
            ("countEndorsements", hit.get("countEndorsements")),
            ("countWarnings", hit.get("countWarnings")),
            ("countDeprecations", hit.get("countDeprecations")),
            ("ranking", hit.get("ranking")),
        ])





    # ----------------------------------------------------------------------
    # API Helpers
    # ----------------------------------------------------------------------
    def _request(self, endpoint, params=None, method="GET", json_body=None):
        """
        Send one HTTP request to the Data Catalog API and return parsed JSON.

        Parameters
        ----------
        endpoint : str
            Path under the API prefix, e.g. "views", "view-details",
            "search/metadata".
        params : dict, optional
            URL query parameters (GET-style).
        method : str, default "GET"
            HTTP method ("GET" or "POST").
        json_body : dict, optional
            JSON request body (used by POST endpoints such as search).

        Returns
        -------
        dict or list
            Parsed JSON response.
        """
        url = f"{self.base_url}/denodo-data-catalog/public/api/{endpoint}"

        response = requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=self.headers,
            verify=self.verify_ssl,
            timeout=120,
        )
        response.raise_for_status()
        return response.json()


    # ----------------------------------------------------------------------
    # DSI Interface: Table Access
    # ----------------------------------------------------------------------
    def get_table(self, table_name, dict_return=False, **kwargs):
        """
        Return one table.

        `table_name` : str
            Name of a registered table (a key of self.schemas).
        `dict_return` : bool, default False
            True -> column-oriented OrderedDict; False -> pandas DataFrame.
            An empty table still has every column of its schema.
        """
        if table_name not in self.schemas:
            raise ValueError(
                f"Table '{table_name}' not found. "
                f"Available tables: {list(self.schemas)}"
            )
        table = self._cache.get(table_name) or \
            self._rows_to_table([], self.schemas[table_name])

        if dict_return:
            return table
        return pd.DataFrame(table, columns=self.schemas[table_name])


    def list(self, collection=False, **kwargs):
        """
        `collection` : bool, default False
            True -> return the table names; False -> print each table's size.
        """
        table_names = list(self._cache.keys())
        if collection:
            return table_names

        for name in table_names:
            df = self.get_table(name)
            print(f"\nTable: {name}")
            print(f"  - num of columns: {df.shape[1]}")
            print(f"  - num of rows: {df.shape[0]}")
        print()


    def num_tables(self, **kwargs):
        """Print and return the number of loaded tables."""
        count = len(self._cache)
        print(f"Database now has {count} table{'' if count == 1 else 's'}")
        return count


    def get_schema(self):
        """
        Return a CREATE TABLE-style description of every registered table.
        Column types are inferred from the first non-null value (TEXT if none).
        """
        statements = []
        for name, columns in self.schemas.items():
            table = self._cache.get(name, {})
            lines = []
            for col in columns:
                dtype = "TEXT"
                for value in table.get(col, []):
                    if value is None:
                        continue
                    if isinstance(value, bool):       # check bool before int
                        dtype = "BOOLEAN"
                    elif isinstance(value, int):
                        dtype = "INTEGER"
                    elif isinstance(value, float):
                        dtype = "REAL"
                    break
                lines.append(f"    {col} {dtype}")
            statements.append(f"CREATE TABLE {name} (\n" + ",\n".join(lines) + "\n);")
        return "\n\n".join(statements)


    def process_artifacts(self, **kwargs):
        """Return all loaded tables: {table_name: column-oriented OrderedDict}."""
        return self._cache



    # ----------------------------------------------------------------------
    # DSI Interface: Summary / Display
    # ----------------------------------------------------------------------
    def summary(self, table_name=None, **kwargs):
        """
        Per-column statistics (same style as the RCSBPDB backend).

        `table_name` : str, optional
            Given -> one DataFrame for that table.
            None  -> [table_names, df1, df2, ...] for every table.
        """
        if table_name is not None:
            return self._summarize_dataframe(self.get_table(table_name))

        table_names = self.list(collection=True)
        return [table_names] + [
            self._summarize_dataframe(self.get_table(name)) for name in table_names
        ]


    @staticmethod
    def _summarize_dataframe(df):
        """One row per column: type, unique count, and min/max/avg/std_dev."""
        rows = []
        for column in df.columns:
            non_null = df[column].dropna()
            row = {
                "column": column,
                "type": str(df[column].dtype).upper(),
                "unique": int(non_null.nunique()),
                "min": None, "max": None, "avg": None, "std_dev": None,
            }

            numeric = pd.to_numeric(non_null, errors="coerce").dropna()
            if not non_null.empty and len(numeric) == len(non_null):
                # every value is a number
                row.update(min=numeric.min(), max=numeric.max(),
                           avg=numeric.mean(), std_dev=numeric.std())
            elif not non_null.empty and non_null.astype(str).str.len().max() <= 80:
                # short text: alphabetical min/max (skip long text like description)
                try:
                    row.update(min=non_null.min(), max=non_null.max())
                except TypeError:
                    pass
            rows.append(row)

        return pd.DataFrame(
            rows, columns=["column", "type", "unique", "min", "max", "avg", "std_dev"]
        )


    def display(self, table_name, num_rows=25, display_cols=None, **kwargs):
        """
        Return the first `num_rows` rows of a table.

        `display_cols` : list of str, optional
            Only these columns (errors if one does not exist).
        """
        df = self.get_table(table_name)
        if df.empty:
            raise ValueError(f"Table '{table_name}' is empty.")

        if display_cols is not None:
            missing = [c for c in display_cols if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Column(s) {missing} not found in '{table_name}'. "
                    f"Available columns: {list(df.columns)}"
                )
            df = df[display_cols]

        # core.py reads attrs["max_rows"] to print "showing N of <total>",
        # so it must be the row count BEFORE truncation (ndp.py precedent).
        # Set it AFTER head(): older pandas does not carry attrs through a slice.
        total_rows = len(df)

        if num_rows:
            df = df.head(num_rows)

        df.attrs["max_rows"] = total_rows
        return df





    # ----------------------------------------------------------------------
    # DSI Interface: Find
    # ----------------------------------------------------------------------
    def find(self, query_object, **kwargs):
        """Search table names, column names and cell values."""
        return (
            self.find_table(query_object)
            + self.find_column(query_object)
            + self.find_cell(query_object)
        )


    def find_table(self, query_object, **kwargs):
        """Tables whose name contains `query_object` (case-insensitive)."""
        if not isinstance(query_object, str):
            return []

        matches = []
        for name, table in self._cache.items():
            if query_object.lower() in name.lower():
                val = ValueObject()
                val.t_name = name
                val.c_name = list(table.keys())
                val.value = table
                val.type = "table"
                matches.append(val)
        return matches


    def find_column(self, query_object, range=False, **kwargs):
        """
        Columns whose name contains `query_object` (case-insensitive).

        `range` : bool, default False
            True -> for all-numeric columns, value is [min, max]
            instead of the full column data.
        """
        if not isinstance(query_object, str):
            return []

        matches = []
        for name, table in self._cache.items():
            for col, values in table.items():
                if query_object.lower() not in col.lower():
                    continue

                val = ValueObject()
                val.t_name = name
                val.c_name = [col]
                val.value = values
                val.type = "column"

                if range:
                    series = pd.Series(values).dropna()
                    numeric = pd.to_numeric(series, errors="coerce").dropna()
                    if len(numeric) and len(numeric) == len(series):
                        val.value = [numeric.min(), numeric.max()]

                matches.append(val)
        return matches


    def find_cell(self, query_object, row=False, **kwargs):
        """
        Cells equal to `query_object`, or (for strings) containing it,
        case-insensitive.

        `row` : bool, default False
            False -> one result per matching cell (value = the cell).
            True  -> one result per matching row (value = list of the
                     row's values, c_name = all columns). dsi.search()
                     uses row=True.
        """
        is_str = isinstance(query_object, str)
        query_lower = query_object.lower() if is_str else None

        matches = []
        for name, table in self._cache.items():
            cols = list(table.keys())
            for row_num, values in enumerate(zip(*table.values())):
                hit_cols = [
                    col for col, cell in zip(cols, values)
                    if cell == query_object
                    or (is_str and isinstance(cell, str) and query_lower in cell.lower())
                ]
                if not hit_cols:
                    continue

                if row:
                    val = ValueObject()
                    val.t_name = name
                    val.c_name = cols
                    val.row_num = row_num
                    val.value = list(values)
                    val.type = "row"
                    matches.append(val)
                else:
                    for col in hit_cols:
                        val = ValueObject()
                        val.t_name = name
                        val.c_name = [col]
                        val.row_num = row_num
                        val.value = values[cols.index(col)]
                        val.type = "cell"
                        matches.append(val)
        return matches



    # ----------------------------------------------------------------------
    # DSI Interface: Relations
    # ----------------------------------------------------------------------
    def find_relation(self, column_name, relation, **kwargs):
        """
        Rows where `column_name` satisfies `relation`
        (DSI parses dsi.find("ranking > 5") into ("ranking", "> 5")).

        Supported relations: > < >= <= == = != , ~ / ~~ (contains),
        and (low, high) for an inclusive range.
        """
        operator, value = self._parse_relation(relation)

        matches = []
        for name, table in self._cache.items():
            if column_name not in table:
                continue
            df = pd.DataFrame(table)
            if df.empty:
                continue

            series = df[column_name]
            if operator in {">", "<", ">=", "<=", "range"}:
                series = pd.to_numeric(series, errors="coerce")   # non-numbers -> NaN -> no match

            for idx, row in df[self._relation_mask(series, operator, value)].iterrows():
                val = ValueObject()
                val.t_name = name
                val.c_name = list(df.columns)
                val.row_num = int(idx)
                val.value = row.tolist()
                val.type = "row"
                matches.append(val)
        return matches


    @staticmethod
    def _relation_mask(series, operator, value):
        """True/False per row for one parsed relation."""
        if operator == ">":
            return series > value
        if operator == "<":
            return series < value
        if operator == ">=":
            return series >= value
        if operator == "<=":
            return series <= value
        if operator == "==":
            return series == value
        if operator == "!=":
            return series != value
        if operator == "contains":
            return series.astype(str).str.contains(str(value), case=False, na=False, regex=False)
        if operator == "range":
            low, high = value
            return (series >= low) & (series <= high)
        raise ValueError(f"Unknown operator: {operator}")


    def _parse_relation(self, relation):
        """
        '> 5' -> ('>', 5)      "~~ 'climate'" -> ('contains', 'climate')
        '= x' -> ('==', 'x')   "(2, 5)"       -> ('range', (2, 5))
        """
        relation = relation.strip()

        # two-character operators first, so '>=' is not read as '>'
        for op, name in ((">=", ">="), ("<=", "<="), ("==", "=="),
                         ("!=", "!="), ("~~", "contains")):
            if relation.startswith(op):
                return name, self._parse_value(relation[2:])
        for op, name in ((">", ">"), ("<", "<"), ("=", "=="), ("~", "contains")):
            if relation.startswith(op):
                return name, self._parse_value(relation[1:])

        if relation.startswith("(") and relation.endswith(")"):
            parts = relation[1:-1].split(",")
            if len(parts) == 2:
                return "range", (self._parse_value(parts[0]), self._parse_value(parts[1]))

        raise ValueError(f"Unknown relation format: {relation}")


    @staticmethod
    def _parse_value(value_str):
        """Strip quotes; turn '5' into 5 and '2.5' into 2.5; leave text as text."""
        value_str = str(value_str).strip()
        if len(value_str) >= 2 and value_str[0] == value_str[-1] and value_str[0] in "'\"":
            value_str = value_str[1:-1]
        try:
            return float(value_str) if "." in value_str else int(value_str)
        except ValueError:
            return value_str


    # ----------------------------------------------------------------------
    # DSI Interface: Lifecycle / Read-only
    # ----------------------------------------------------------------------
    def close(self):
        """Clear all loaded data; registered tables go back to empty."""
        self._cache = OrderedDict(
            (name, self._rows_to_table([], columns))
            for name, columns in self.schemas.items()
        )
        self._view_map = {}
        self._loaded = False


    def query_artifacts(self, query, **kwargs):
        """**Not supported** - tables are in memory, there is no SQL engine."""
        raise NotImplementedError(
            "Denodo backend does not support SQL query(). "
            "Use find(), search() or get_table() instead."
        )


    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        """**Not supported** - Denodo backend is read-only."""
        raise NotImplementedError("Denodo backend is read-only")

