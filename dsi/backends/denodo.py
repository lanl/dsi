"""
Denodo Data Catalog Backend for DSI

Read-only backend that pulls metadata from a Denodo Data Catalog REST API
and exposes it as in-memory DSI tables. Which tables fill depends on the
``params`` passed to the backend:

=========================  ==========================================  ========================
Table                      Filled by                                   Cost
=========================  ==========================================  ========================
``denodo_search_results``  ``{"keywords": "..."}``                     a few requests
``denodo_databases``       ``{"databases": []}``                       two requests
``denodo_views``           ``{"views": True}``                         a few dozen requests
``denodo_columns``         ``{"columns": <scope>}``                    one request per view
``denodo_properties``      ``{"columns": <scope>}``                    the same request
=========================  ==========================================  ========================

Every table is created with all of its columns, whether or not it holds
rows, so ``summary()`` and ``display()`` work before anything is fetched.

``denodo_columns`` and ``denodo_properties`` come from the same per-view
request, so one harvest fills both. That request takes roughly a second
per view, which is why a scope is required rather than defaulted: a
keyword, ``{"database": "<name>"}``, or a list of ``"database.view"``
names. Progress is printed while it runs, and views whose details could
not be fetched are recorded on ``backend.failed_views``.

Nothing site-specific is stored in this file. The catalog URL and the
OAuth client settings resolve as **argument, then environment variable,
then** ``~/.denodo/config.json``.

Authentication uses the OAuth 2.0 authorization-code flow and opens a
browser on first use. Pass ``token=...`` to reuse a token you already
hold. Tokens expire after about an hour and are not refreshed, so build
a new backend rather than expecting one to renew itself.

See ``examples/backends/denodo/`` for runnable examples and a full
description of every column.
"""

import os
import time
import webbrowser
import json
import html          # for normalization,  for unescape
import logging       # for normalization, because the contract requires URLs it can't classify to be logged for review, never silently discarded.
import re            # for normalization, for the tag and href patterns
import difflib

from pathlib import Path

from collections import OrderedDict, Counter
from datetime import datetime, timezone
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
# Normalization (DATA_CONTRACT section 3)
#
# Pure functions: value in, value out. Every normalization rule in this
# file lives here, so nothing downstream re-implements them.
# ----------------------------------------------------------------------
logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")
_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

# Descriptions carry bare URLs ("Oracle Source: https://..."), property
# values carry anchors. Both forms must be matched or the documentation
# URL is silently lost (contract 3.3a).
_URL_RE = re.compile(r'https?://[^\s<>"\'\\]+')
_URL_TRAILING = ".,;:!?"

# Values that mean "nothing is recorded here" (contract 3.4)
_NULL_VALUES = {"", "NOLINK"}


def normalize_property_value(visual_value, property_type):
    """
    Normalize one property value or view description (contract 3.2).

    The order of the steps is load-bearing:

    1. Tags are stripped **only** when `property_type == "RICH_TEXT"`.
       The type is a guard, not a detector: a RICH_TEXT value may be
       plain text, and stripping plain text is a harmless no-op.
    2. `html.unescape()` runs **after** stripping, for every type.
       Entities occur in tag-free values, and unescaping first would
       turn `&lt;b&gt;` into a real tag that stripping then deletes,
       losing the text it wrapped.
    3. Whitespace runs collapse to one space; an empty result is None.

    Parameters
    ----------
    visual_value : str or None
        A property entry's `visualValue`, or a view description.
    property_type : str or None
        `ENUMERATION`, `LONG_TEXT` or `RICH_TEXT`.

    Returns
    -------
    str or None
    """
    if visual_value is None:
        return None

    text = _TAG_RE.sub("", visual_value) if property_type == "RICH_TEXT" else visual_value
    text = html.unescape(text)

    return " ".join(text.split()) or None


def extract_urls(visual_value):
    """
    Pull every URL out of a raw value (contract 3.2 step 2, 3.3a).

    Must run **before** any tag stripping: URLs in attributes are deleted
    along with the markup.

    Two forms occur, and matching only one returns [] for the other with
    no error: property values use anchors, descriptions use bare text.
    href targets come first so an anchor still wins when both appear.

    Returns
    -------
    list of str
        Unescaped URLs in the order they appear; empty if there are none.
    """
    if not visual_value:
        return []

    urls = [html.unescape(url) for url in _HREF_RE.findall(visual_value)]

    for raw in _URL_RE.findall(visual_value):
        # Trailing sentence punctuation is not part of the URL; ")" and "/"
        # can be, so they are left alone.
        url = html.unescape(raw).rstrip(_URL_TRAILING)
        if url not in urls:
            urls.append(url)

    return urls



def null_if_empty(value):
    """
    Apply the null policy (contract 3.4).

    `""`, `"NOLINK"` and whitespace-only strings all mean "no value".
    """
    if value is None:
        return None

    text = value.strip() if isinstance(value, str) else value

    return None if text in _NULL_VALUES else text


def canonical_property_name(group_name, property_name):
    """
    Build the canonical property name (contract 3.5).

    `f"{groupName}/{propertyName}"` exactly as returned, **including
    trailing punctuation**. No trimming, no case folding, no merging:
    two names differing by one character are two different properties.
    """
    return f"{group_name}/{property_name}"


def normalize_description(description):
    """
    Split a view description into clean text and its documentation URL.

    The documentation URL is taken **structurally** -- it is the URL
    embedded in the description -- rather than by matching hostnames,
    so no site-specific value appears in this file (amends contract 3.3).
    A description carrying more than one URL is logged rather than
    silently reduced, which is what 3.3 asks for.

    Returns
    -------
    (str or None, str or None)
        The normalized description, and the documentation URL.
    """
    urls = extract_urls(description)
    text = normalize_property_value(description, "RICH_TEXT")

    if len(urls) > 1:
        logger.info(
            "Description carries %d URLs; keeping the first as documentation_url: %s",
            len(urls), urls,
        )

    return text, (urls[0] if urls else None)



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

    # One row per database, from GET database-management/user/databases.
    # Column order here = column order in the DSI table.
    DATABASES_SCHEMA: ClassVar[list[str]] = [
        "db_name",
        "description",
        "database_id",    # Denodo's own databaseId -- source-assigned and
                          # non-contiguous, not a row number
        "server_id",      # a field of the response body, not just the request
        "view_count",     # counted from the views endpoint, not from this one
        "fetched_at",     # provenance
        "source_env",     # provenance
    ]

    # One row per view, sourced from POST /search/metadata (contract 2.2, rev 21):
    # 45 requests for the whole catalogue, where one view-details call per view
    # would cost 4,418.
    VIEWS_SCHEMA: ClassVar[list[str]] = [
        "view_name",          # key part 1
        "db_name",            # key part 2
        "description",        # normalized; None for the ~19% search leaves empty
        "documentation_url",  # extracted from the description (contract 3.3a)
        "categories",
        "tags",
        "element_id",         # Denodo's own id -- differs between environments
        "last_modified_at",
        "fetched_at",         # provenance
        "source_env",         # provenance
    ]

    # One row per column of each view, from view-details.schema (contract 2.4).
    COLUMNS_SCHEMA: ClassVar[list[str]] = [
        "view_name",          # key part 1
        "db_name",            # key part 2
        "column_name",        # key part 3
        "ordinal_position",   # index in the schema list, 0-based
        "data_type",          # schema[].type
        "description",        # schema[].description, "" -> None
        "fetched_at",         # provenance
        "source_env",        
    ]

    # One row per custom property, long EAV form (contract 2.5, Decision 2).
    # EAV because the property set drifts without schema migrations: prod has
    # 43 property groups, 36 of them currently unpopulated.
    PROPERTIES_SCHEMA: ClassVar[list[str]] = [
        "view_name",          # key part 1
        "db_name",            # key part 2
        "property_name",      # key part 3: "groupName/propertyName", verbatim
        "property_value",     # visualValue, normalized per contract 3.2
        "fetched_at",         # provenance
        "source_env",         # provenance
    ]


    SUPPORTED_PARAMS: ClassVar[set[str]] = {
        "keywords",      # text to search ("" = whole catalog)
        "search_in",     # name | description | properties | column_names | column_descriptions
        "match",         # substring | all_words | any_words
        "categories",    # category ids
        "tags",          # tag ids
        "limit",         # max rows (default: every hit)
        "database",      # retired: raises with a pointer to 'views' / 'columns'
        "view",          # retired: raises with a pointer to 'views' / 'columns'

        "databases",      # database names; [] = every visible one -> denodo_databases
        "views",          # True for the whole catalogue, or a keyword -> denodo_views
        "columns",       #  required scope -> denodo_columns + denodo_properties

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
    # Harvest tuning. SECONDS_PER_VIEW was measured on prod 2026-10-02 over a
    # 10-view sample; it is only used for the up-front time estimate.
    # ----------------------------------------------------------------------
    SECONDS_PER_VIEW: ClassVar[float] = 1.3
    PROGRESS_EVERY: ClassVar[int] = 50
    MAX_CONSECUTIVE_FAILURES: ClassVar[int] = 10
    # ~1.3 s per view against a token that lives about an hour; 1,500
    # views is roughly 32 minutes, comfortably inside it (O-28).
    MAX_SCOPE_VIEWS: ClassVar[int] = 1500


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
        # Views whose view-details call failed, as (db_name, view_name) pairs.
        # Declared before any early return so even a probe has it: the printed
        # harvest summary scrolls away, and denodo_views has no fetch_status
        # column to carry failures (contract 4.1).
        self.failed_views = []


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

        # Provenance (Principle #6). Same precedence as every other setting:
        # explicit argument > environment variable > config file. The host
        # actually talked to is the last resort, so the column is never null
        # and nothing site-specific enters this file.
        self.source_env = (                             # This is for database table
            kwargs.get("source_env")
            or _setting("source_env", env_var="DENODO_SOURCE_ENV")
            or urlparse(self.base_url).netloc
        )


        self.headers = {}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        # Table registry: table name -> its columns (the shape).
        # Three tables implemented; denodo_columns and denodo_properties
        # are added here as their schemas are defined.
        self.schemas = {
            "denodo_search_results": self.SEARCH_SCHEMA,
            "denodo_databases": self.DATABASES_SCHEMA,
            "denodo_views": self.VIEWS_SCHEMA,
            "denodo_columns": self.COLUMNS_SCHEMA,
            "denodo_properties": self.PROPERTIES_SCHEMA,
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
    def _validate_params(self, params):
        """
        Reject unknown parameter keys before any request is made.

        SUPPORTED_PARAMS was declared from the start but nothing enforced it,
        so a typo such as {"veiws": True} fell through to the search branch
        and quietly searched the whole catalog into the wrong table. Raising
        here turns a silent wrong answer into an obvious error.

        Parameters
        ----------
        params : dict
            One query dict.

        Raises
        ------
        ValueError
            If any key is not in SUPPORTED_PARAMS.
        """
        unknown = sorted(set(params) - self.SUPPORTED_PARAMS)
        if not unknown:
            return

        hints = []
        for key in unknown:
            close = difflib.get_close_matches(key, self.SUPPORTED_PARAMS, n=1)
            hints.append(f"'{key}'" + (f" (did you mean '{close[0]}'?)" if close else ""))

        raise ValueError(
            f"Unsupported Denodo params: {', '.join(hints)}. "
            f"Supported: {sorted(self.SUPPORTED_PARAMS)}"
        )   


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
        # Validate every query before fetching anything: a typo in the third
        # query should not be discovered after the first two have already
        # hit the network.
        for query_params in query_list:
            self._validate_params(query_params)


        # Collect results from all queries

        search_rows = []   # search path -> flat rows from the POST response only
        database_rows = []  # databases path -> one row per database
        view_rows = []     # views path -> one row per view, from the search harvest
        column_rows = []    # columns path -> one row per column of each view
        property_rows = []  # columns path -> one row per custom property


        for query_params in query_list:
            # 'view' and 'database' date from the original design, where every
            # table came from one view-details call per view. Both tables they
            # fed now come from cheaper sources, so they are refused with a
            # pointer instead of silently doing nothing.
            if "view" in query_params or "database" in query_params:
                raise ValueError(
                    "'view' and 'database' are no longer supported. Use "
                    "params={'views': True} for the view table, or "
                    "params={'columns': ['database.view', ...]} for the columns "
                    "and custom properties of specific views."
                )

            elif "databases" in query_params:
                # Layer 1: one row per database. Independent of the
                # view-details path, so it needs no view fetches.
                database_rows.extend(
                    self._build_database_rows(query_params["databases"])
                )
            elif "views" in query_params:
                # Same POST the search path uses, but mapped to the contract's
                # column names. 45 requests for the whole catalogue, where one
                # view-details call per view would cost 4,418 (contract 2.2).
                wanted = query_params["views"]
                flat = self._run_single_query(
                    {"keywords": wanted if isinstance(wanted, str) else ""}
                )
                view_rows.extend(self._build_view_rows(flat))
            elif "columns" in query_params:
                # One harvest feeds both tables: schema[] and propertyInfo
                # arrive in the same view-details response (contract 2.4, 2.5).
                targets = self._resolve_scope(query_params["columns"])
                details, failed = self._harvest_view_details(targets)
                self.failed_views.extend(failed)

                columns, properties = self._extract_tables(details)
                column_rows.extend(columns)
                property_rows.extend(properties)           
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
        if database_rows:
            self._cache["denodo_databases"] = self._rows_to_table(
                database_rows, self.DATABASES_SCHEMA
            )
        if view_rows:
            # Composite key (db_name, view_name) -- Principle #2. Verified
            # unique across all 4,418 views on prod, 2026-09-29.
            seen, unique_views = set(), []
            for row in view_rows:
                key = (row["db_name"], row["view_name"])
                if key not in seen:
                    seen.add(key)
                    unique_views.append(row)

            self._cache["denodo_views"] = self._rows_to_table(
                unique_views, self.VIEWS_SCHEMA
            )
        if column_rows:
            self._cache["denodo_columns"] = self._rows_to_table(
                column_rows, self.COLUMNS_SCHEMA
            )

        if property_rows:
            self._cache["denodo_properties"] = self._rows_to_table(
                property_rows, self.PROPERTIES_SCHEMA
            )

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


   


    def _get_databases(self):                                                # For Database table
        """
        List every database the caller can see, with its description.

        One call returns names and descriptions together. The older
        route -- one browse/databases lookup per name -- returns the
        same values one at a time.

        Returns
        -------
        list of dict
            {databaseId, serverId, databaseName, description, descriptionType}
        """
        result = self._request(
            "database-management/user/databases",
            params={"serverId": self.server_id},
        )

        if isinstance(result, list):
            return result
        return result.get("databases", result.get("elements", []))


    def _get_view_counts(self):                                         # For Database table
        """
        Count views per database from a single call to the list endpoint.

        The list endpoint ignores its databaseName parameter and always
        returns the whole catalog, so one call is the cheapest and the
        most reliable way to count. The per-database route does filter
        correctly, but its response carries no total, so counting
        through it costs one request per page per database.

        Returns
        -------
        collections.Counter
            {db_name: view_count}. A database with no views is absent
            from the Counter and reads back as 0.
        """
        result = self._request("views", params={"serverId": self.server_id})

        if isinstance(result, list):
            all_views = result
        else:
            all_views = result.get("views", result.get("elements", []))

        # "db" is the list endpoint's spelling; view-details calls the
        # same concept "databaseName".
        return Counter(v.get("db") for v in all_views if not v.get("deleted"))


    def _build_database_rows(self, names=None):                     # For Database table
        """
        Build the rows of denodo_databases.

        Parameters
        ----------
        names : list of str, optional
            Restrict the table to these databases. None or an empty
            list means every database the caller can see.

        Returns
        -------
        list of dict
            One dict per database, keyed by DATABASES_SCHEMA columns.

        Raises
        ------
        ValueError
            If a requested name is not one of the caller's databases.
            An unknown name must never reach the per-database views
            route: that route answers 500 with an internal query
            fragment rather than a clean 404.
        """
        databases = self._get_databases()
        available = {db.get("databaseName") for db in databases}

        if names:
            unknown = sorted(set(names) - available)
            if unknown:
                raise ValueError(
                    f"Unknown database(s): {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(available))}."
                )
            wanted = set(names)
        else:
            wanted = available

        counts = self._get_view_counts()
        fetched_at = datetime.now(timezone.utc).isoformat()

        rows = []
        for db in databases:
            db_name = db.get("databaseName")
            if db_name not in wanted:
                continue
            rows.append({
                "db_name": db_name,
                # "no description" arrives as None from this endpoint and
                # as "" from browse/databases; one helper absorbs both.
                "description": null_if_empty(db.get("description")),
                "database_id": db.get("databaseId"),
                "server_id": db.get("serverId", self.server_id),
                "view_count": counts[db_name],
                "fetched_at": fetched_at,
                "source_env": self.source_env,
            })

        return rows

    def _build_view_rows(self, rows):                                # For Views table
        """
        Build the denodo_views rows from search results (contract 2.2, rev 21).

        Takes rows already flattened to SEARCH_SCHEMA by _run_single_query,
        not raw hits. The search response carries description, categories and
        tags for the whole catalogue in 45 requests, where one view-details
        call per view would cost 4,418.

        The column schema and custom properties are not in this response, so
        denodo_columns and denodo_properties are not built here.

        Parameters
        ----------
        rows : list of dict
            Flattened search rows, keyed by SEARCH_SCHEMA columns.

        Returns
        -------
        list of dict
            One dict per view, keyed by VIEWS_SCHEMA columns.
        """
        fetched_at = datetime.now(timezone.utc).isoformat()

        view_rows = []
        for row in rows:
            # Returns (text, url). The URL stays in the text for bare-URL
            # descriptions -- removing it would leave "Oracle Source: "
            # dangling, and 3.2 says never silently discard content.
            description, documentation_url = normalize_description(
                row.get("description")
            )

            view_rows.append({
                "view_name": row.get("name"),
                "db_name": row.get("database_name"),
                "description": description,
                "documentation_url": documentation_url,
                # Already joined with ", " and emptied to None by _flatten_hit.
                # Reused as-is so this table and denodo_search_results can
                # never disagree about the same data.
                "categories": row.get("categories"),
                "tags": row.get("tags"),
                "element_id": row.get("id"),
                "last_modified_at": row.get("lastModificationVdpData"),
                "fetched_at": fetched_at,
                "source_env": self.source_env,
            })

        return view_rows

    def _resolve_scope(self, spec):                                  # For Phase 2c
        """
        Turn a user-supplied scope into a list of (db_name, view_name) pairs.

        Required because a per-view harvest costs ~1.3 s per view: the whole
        catalogue would take about 92 minutes, so the caller must say what
        they want (contract D-8).

        Parameters
        ----------
        spec : str or dict or list
            - str                     : views matching this keyword
            - {"database": "<name>"}  : every view in one database
            - ["db.view", ...]        : exactly these views

        Returns
        -------
        list of tuple
            (db_name, view_name) pairs, de-duplicated, in a stable order.

        Raises
        ------
        ValueError
            If the scope is empty, malformed, or names an unknown database.
        """
        # Explicit list: no request at all.
        if isinstance(spec, (list, tuple)):
            targets = []
            for entry in spec:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    targets.append(tuple(entry))
                elif isinstance(entry, str) and "." in entry:
                    db_name, _, view_name = entry.partition(".")
                    targets.append((db_name, view_name))
                else:
                    raise ValueError(
                        f"Cannot read scope entry {entry!r}. Use 'database.view' "
                        "or (database, view)."
                    )

        # One database: one /views call, filtered here.
        elif isinstance(spec, dict) and "database" in spec:
            db_name = spec["database"]
            result = self._request("views", params={"serverId": self.server_id})
            all_views = result if isinstance(result, list) else result.get("views", [])

            targets = [(v.get("db"), v.get("name"))
                       for v in all_views
                       if v.get("db") == db_name and not v.get("deleted")]
            if not targets:
                available = sorted({v.get("db") for v in all_views if v.get("db")})
                raise ValueError(
                    f"No views found in database '{db_name}'. "
                    f"Databases with views: {', '.join(available)}."
                )

        # Keyword: the search harvest already knows how to find views.
        elif isinstance(spec, str) and spec.strip():
            rows = self._run_single_query({"keywords": spec})
            targets = [(row.get("database_name"), row.get("name")) for row in rows]

        else:
            raise ValueError(
                "The 'columns' scope is required and cannot be empty. Pass a "
                "keyword, {'database': '<name>'}, or a list of 'database.view' "
                "names. A full-catalogue harvest takes about 92 minutes and is "
                "not available as a default."
            )

        # Stable order, no duplicates: the harvest cost makes a repeat expensive.
        targets = list(dict.fromkeys(targets))
        # A valid scope that matches nothing is a different failure from an
        # empty scope, and the user cannot tell a typo from an empty catalog
        # once two blank tables come back.
        if not targets:
            raise ValueError(
                f"Scope {spec!r} matched no views. Check the keyword or the "
                "view names -- a scope that matches nothing produces empty "
                "tables with no other explanation."
            )
        # The token lives about an hour and a harvest runs at ~1.3 s per view,
        # so a scope past this size cannot finish before it expires (O-28).
        # Refuse up front rather than fail halfway and return a partial table.
        if len(targets) > self.MAX_SCOPE_VIEWS:
            minutes = len(targets) * self.SECONDS_PER_VIEW / 60
            raise ValueError(
                f"Scope resolves to {len(targets)} views, about {minutes:.0f} "
                f"minutes -- longer than the access token lives. Narrow the "
                f"scope, or split it across several runs of at most "
                f"{self.MAX_SCOPE_VIEWS} views."
            )

        return targets



    def _harvest_view_details(self, targets):                        # For Phase 2c
        """
        Fetch view-details for every target, reporting progress as it goes.

        One call per view at roughly 1.3 s each, so this is the only path in
        the backend where a user waits minutes rather than seconds. Silence
        would be indistinguishable from a hang (contract O-42), so the count
        and expected duration are printed before any request is made.

        Parameters
        ----------
        targets : list of tuple
            (db_name, view_name) pairs from _resolve_scope.

        Returns
        -------
        tuple
            (details, failed) -- the view-details dicts that came back, and
            the (db_name, view_name) pairs that did not.
        """
        total = len(targets)
        seconds = total * self.SECONDS_PER_VIEW
        estimate = (f"{seconds:.0f} seconds" if seconds < 90
                    else f"{seconds / 60:.0f} minutes")
        print(f"Fetching details for {total} view(s), about {estimate}...")

        start = time.perf_counter()
        details, failed = [], []
        consecutive = 0

        for index, (db_name, view_name) in enumerate(targets, start=1):
            detail = self._get_view_details(view_name, db_name)

            if detail is None:
                failed.append((db_name, view_name))
                consecutive += 1
                # A run of failures is systemic -- an expired token or a server
                # outage -- not one bad view. Stopping saves the remaining hour.
                if consecutive >= self.MAX_CONSECUTIVE_FAILURES:
                    print(f"  stopped at {index}/{total} after {consecutive} "
                          f"consecutive failures; the token may have expired")
                    break
            else:
                details.append(detail)
                consecutive = 0

            if index % self.PROGRESS_EVERY == 0 or index == total:
                elapsed = (time.perf_counter() - start) / 60
                print(f"  {index:>5} / {total}   ({elapsed:.1f} min elapsed)")

        elapsed = (time.perf_counter() - start) / 60
        print(f"  done in {elapsed:.1f} min -- {len(details)} ok, {len(failed)} failed")

        if failed:
            shown = ", ".join(f"{db}.{view}" for db, view in failed[:5])
            more = f", and {len(failed) - 5} more" if len(failed) > 5 else ""
            print(f"  failed: {shown}{more}")

        return details, failed

    def _extract_tables(self, details):                              # For Phase 2c
        """
        Split view-details responses into column rows and property rows.

        Both tables come from the same response, so one harvest feeds both:
        schema[] becomes denodo_columns, and the three propertyInfo maps
        become denodo_properties in long EAV form (contract 2.4, 2.5).

        Parameters
        ----------
        details : list of dict
            view-details responses from _harvest_view_details.

        Returns
        -------
        tuple
            (column_rows, property_rows), each a list of dicts keyed by the
            matching schema.
        """
        fetched_at = datetime.now(timezone.utc).isoformat()
        column_rows, property_rows = [], []

        for detail in details:
            view_name = detail.get("name")
            db_name = detail.get("databaseName")

            # --- columns -------------------------------------------------
            for position, field in enumerate(detail.get("schema") or []):
                column_rows.append({
                    "view_name": view_name,
                    "db_name": db_name,
                    "column_name": field.get("name"),
                    "ordinal_position": position,
                    "data_type": field.get("type"),
                    "description": null_if_empty(field.get("description")),
                    "fetched_at": fetched_at,
                    "source_env": self.source_env,
                })

            # --- properties ----------------------------------------------
            # Three maps, same shape: {group name: [property, ...]}. Any of
            # them can be absent or None on a given view (contract 4.2).
            property_info = detail.get("propertyInfo") or {}
            for map_name in ("summaryPropertyMap", "generalTabPropertyMap",
                             "customTabPropertyMap"):
                for group_name, items in (property_info.get(map_name) or {}).items():
                    for item in items or []:
                        value = normalize_property_value(
                            item.get("visualValue"), item.get("propertyType")
                        )
                        # EAV stores what exists. An unset property adds no
                        # information and would cost tens of thousands of rows.
                        if value is None:
                            continue

                        property_rows.append({
                            "view_name": view_name,
                            "db_name": db_name,
                            "property_name": canonical_property_name(
                                item.get("groupName", group_name),
                                item.get("propertyName"),
                            ),
                            "property_value": value,
                            "fetched_at": fetched_at,
                            "source_env": self.source_env,
                        })

        return column_rows, property_rows



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
            False -> one result per matching cell, where value is the cell.
            True -> one result per matching row, where value is a list of
            the row's values and c_name is every column. dsi.search() uses
            row=True.
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
        self.failed_views = []
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

