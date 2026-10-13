import json
import os
import re
import shutil
import tempfile

import pandas as pd

from dsi.dsi import DSI
from dsi.utils.dsi_utils import ingest_or_overwrite_table
from dsi.utils.federated.catalog_walk import walk_hpc_catalog
from dsi.utils.federated.source_entry import build_source_entry, webserver_backend_names
from dsi.utils.webserver_query import query_webserver

_CROSSWALK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils")
with open(os.path.join(_CROSSWALK_DIR, "webserver_crosswalk.json")) as _f:
    WEBSERVER_CROSSWALK = json.load(_f)
with open(os.path.join(_CROSSWALK_DIR, "hpc_schema_crosswalk.json")) as _f:
    HPC_SCHEMA_CROSSWALK = json.load(_f)
FED_QUERY_ALLOWED_FIELDS = {"keyword", "author", "title", "doi", "id", "limit"}


class DSIScout:
    '''
    Live federation across DSI's webserver backends (NDP, OSTI, Oceans11, RCSBPDB, Zenodo) and
    HPC-registered catalog chains (CSV catalogs, possibly nested, terminating in a database file).

    Unlike DSIFederated (which queries already-downloaded local DSI databases via raw SQL),
    DSIScout performs live discovery against heterogeneous external sources - reconciling their
    different schemas through webserver_crosswalk.json / hpc_schema_crosswalk.json - using shared
    semantic fields (keyword, author, title, doi, id, limit).

    The actual discovery/merge mechanics (catalog walking, schema-crosswalk filtering, webserver
    querying, source-entry validation) live as plain functions in dsi.utils.federated - this class
    is the stateful orchestrator: it holds registered sources and the result store, and calls into
    those utilities.

    Usable standalone from a script, or as the backing implementation behind the CLI's
    scout_add/scout_list/scout_delete/scout_fetch commands.
    '''

    def __init__(self, sources=None, store_filename=None, verbose=True, **fields):
        """
        Args:
            sources: optional list of sources to register immediately. Each entry is either a
                webserver backend name (str, e.g. "OSTI") or a dict with HPC fields
                (location_type, location, path, submitter_name).
            store_filename: optional filename for the internal DSI Sqlite result store. Defaults
                to a temporary hidden backend, same as DSI()'s own default.
            verbose: if True, prints progress/warning messages during query() (matches today's
                CLI output). If False, query() stays silent except for raised exceptions.
            **fields: optional query fields (keyword, author, title, doi, id, limit). If given,
                query() is run immediately after registering sources.
        """
        self.sources = []
        self.verbose = verbose
        self._store = DSI(filename=store_filename or ".temp_dsi.db", backend_name="Sqlite", silence_messages=True)

        if sources:
            for s in sources:
                if isinstance(s, str):
                    self.add_source(webserver_name=s)
                else:
                    self.add_source(**s)

        if fields:
            self.query(**fields)


    def _log(self, msg):
        if self.verbose:
            print(msg)


    def add_source(self, webserver_name=None, location_type=None, location=None, path=None, submitter_name=""):
        '''
        Registers a webserver backend or HPC source. Raises ValueError on invalid or duplicate input.
        '''
        valid_names = webserver_backend_names(self._store.t.module_collection['backend'])
        entry = build_source_entry(valid_names, webserver_name, location_type, location, path, submitter_name)
        if entry in self.sources:
            raise ValueError("this backend/source is already registered.")
        self.sources.append(entry)
        return entry


    def delete_source(self, webserver_name=None, location_type=None, location=None, path=None, submitter_name=""):
        '''
        Deletes a registered source. Takes the same arguments used in add_source. Raises ValueError
        if no matching entry is found.
        '''
        valid_names = webserver_backend_names(self._store.t.module_collection['backend'])
        entry = build_source_entry(valid_names, webserver_name, location_type, location, path, submitter_name)
        if entry not in self.sources:
            raise ValueError("no matching registered backend/source found.")
        self.sources.remove(entry)
        return entry


    def list_sources(self):
        '''Returns the list of registered source entry dicts.'''
        return list(self.sources)


    def query(self, keyword=None, author=None, title=None, doi=None, id=None, limit=None):
        '''
        Queries all registered webserver backends with shared fields (keyword, author, title, doi,
        id, limit), using the per-backend field/table mappings in webserver_crosswalk.json, and walks
        every registered HPC catalog source, filtering its results using the matched schema's
        field_map in hpc_schema_crosswalk.json before merging. Primary-table results are merged
        (outer join on columns) into a 'fed_results' table, tagged with a synthetic 'fed_id' =
        "<backend>:<original_pk>" (webservers) or "hpc:<location>:<row index>" (HPC). Every other
        table returned by a backend/source is loaded as '<backend>_<table>' or
        'hpc_<location>_<table>'. If that secondary table mixes rows from multiple parent records
        (e.g. NDP/RCSBPDB/Zenodo "resources"), it's tagged with 'fed_parent_id' pointing back to the
        owning fed_id. Backends whose secondary tables are already dedicated to one record (e.g.
        Oceans11 Tier-2 tables) need no such tag. HPC sources are catalog chains of CSV files (same
        schema as federate_datasets.py's repo CSVs) terminating in a database file; that database's
        tables/columns are matched against the known schemas in hpc_schema_crosswalk.json to find its
        primary table (falling back to a resources/records guess, with a warning and no filtering, if
        no schema matches) - that table is filtered by fields and merged into fed_results, and its
        other tables load as secondary tables, unfiltered. No further recursion happens inside that
        terminal database - any internal pointer tables (e.g. Oceans11-style 't2db_url', or a
        'federated'/'remote_location' table) are loaded as plain secondary tables, not followed.

        Results are ingested into this DSIScout's internal store (accessible via list()/summary()/
        display()/get_table() afterward) and also returned as {"fed_results": df, **secondary_tables}.

        Raises ValueError if no sources are registered, or if no key:value fields are given at all.
        '''
        if not self.sources:
            raise ValueError("no federated backends registered. Use add_source first.")

        fields = {}
        for key, value in (("keyword", keyword), ("author", author), ("title", title),
                            ("doi", doi), ("id", id), ("limit", limit)):
            if value is not None:
                fields[key] = value

        if not fields:
            raise ValueError("no query fields given. Supported: " + ", ".join(sorted(FED_QUERY_ALLOWED_FIELDS)))

        combined_primary = None
        secondary_tables = {}  # dest_table_name -> DataFrame

        for entry in self.sources:
            if entry["kind"] != "webserver":
                continue
            primary_df, secondary = query_webserver(entry, fields, WEBSERVER_CROSSWALK, log_fn=self._log)
            backend_name = entry["backend_name"]

            if primary_df is not None and not primary_df.empty:
                if combined_primary is None:
                    combined_primary = primary_df
                else:
                    all_cols = list(dict.fromkeys(list(combined_primary.columns) + list(primary_df.columns)))
                    combined_primary = pd.concat(
                        [combined_primary.reindex(columns=all_cols), primary_df.reindex(columns=all_cols)], ignore_index=True)

            for table_name, df in secondary.items():
                dest_name = f"{backend_name}_{table_name}"
                if dest_name in secondary_tables:
                    prev = secondary_tables[dest_name]
                    all_cols = list(dict.fromkeys(list(prev.columns) + list(df.columns)))
                    secondary_tables[dest_name] = pd.concat(
                        [prev.reindex(columns=all_cols), df.reindex(columns=all_cols)], ignore_index=True)
                else:
                    secondary_tables[dest_name] = df

        hpc_workspace = tempfile.mkdtemp(prefix="dsi_fed_hpc_")
        for entry in self.sources:
            if entry["kind"] != "hpc":
                continue
            location_tag = f"hpc:{entry['location']}"
            safe_location = re.sub(r"[^A-Za-z0-9_]", "_", entry["location"])
            resources_df, secondary = walk_hpc_catalog(
                entry, hpc_workspace, fields, HPC_SCHEMA_CROSSWALK, log_fn=self._log)

            if resources_df is not None and not resources_df.empty:
                df = resources_df.copy()
                df.insert(0, "fed_source", location_tag)
                df.insert(1, "fed_id", [f"{location_tag}:{i}" for i in range(len(df))])

                if combined_primary is None:
                    combined_primary = df
                else:
                    all_cols = list(dict.fromkeys(list(combined_primary.columns) + list(df.columns)))
                    combined_primary = pd.concat(
                        [combined_primary.reindex(columns=all_cols), df.reindex(columns=all_cols)], ignore_index=True)

            for table_name, df in secondary.items():
                dest_name = f"hpc_{safe_location}_{table_name}"
                if dest_name in secondary_tables:
                    prev = secondary_tables[dest_name]
                    all_cols = list(dict.fromkeys(list(prev.columns) + list(df.columns)))
                    secondary_tables[dest_name] = pd.concat(
                        [prev.reindex(columns=all_cols), df.reindex(columns=all_cols)], ignore_index=True)
                else:
                    secondary_tables[dest_name] = df

            self._log(f"'{location_tag}' returned {len(resources_df) if resources_df is not None else 0} resource result(s).")
        shutil.rmtree(hpc_workspace, ignore_errors=True)

        if combined_primary is None:
            self._log("scout_fetch: no results found across any registered webserver backend or HPC source.")
            return {}

        ingest_or_overwrite_table(self._store.t, "fed_results", combined_primary)
        for dest_name, df in secondary_tables.items():
            ingest_or_overwrite_table(self._store.t, dest_name, df)

        self._log(f"\nStored 'fed_results' ({len(combined_primary)} row(s)) and {len(secondary_tables)} related table(s): "
                   f"{', '.join(secondary_tables.keys()) if secondary_tables else 'none'}")
        self._log("Tables prefixed by backend name hold each source's related data;\n")

        return {"fed_results": combined_primary, **secondary_tables}


    # ------------------------------------------------------------------
    # Thin delegations to the internal result store, matching DSI's own
    # Python API / the webserver backends' shape.
    # ------------------------------------------------------------------

    def list(self, collection=False):
        return self._store.list(collection=collection)

    def summary(self, table_name=None):
        return self._store.summary(table_name=table_name) if table_name else self._store.summary()

    def display(self, table_name, **kwargs):
        return self._store.display(table_name, **kwargs)

    def get_table(self, table_name, **kwargs):
        return self._store.get_table(table_name, **kwargs)

    def find(self, query_object, **kwargs):
        return self._store.find(query_object, **kwargs)

    def find_table(self, query_object, **kwargs):
        return self._store.find_table(query_object, **kwargs)

    def find_column(self, query_object, **kwargs):
        return self._store.find_column(query_object, **kwargs)

    def find_cell(self, query_object, **kwargs):
        return self._store.find_cell(query_object, **kwargs)

    def find_relation(self, condition_str, **kwargs):
        return self._store.find_relation(condition_str, **kwargs)

    def num_tables(self):
        return self._store.num_tables()

    def close(self):
        return self._store.close()
