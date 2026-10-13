import os

import pandas as pd

from dsi.dsi import DSI
from dsi.utils.federation_utils import csv_to_list_of_dicts


def filter_by_field_map(df, field_map, fields):
    '''
    Filters df using fields (keyword/author/etc. query args) against field_map (a schema's
    {query_field: column_or_[columns]} mapping, e.g. from hpc_schema_crosswalk.json). Each given
    field does a case-insensitive substring match; if a field maps to multiple columns, those are
    OR'd together, and different fields are AND'd together. Fields not present in field_map, or
    whose mapped column(s) aren't actually in df, are ignored (no error - just not filterable here).
    'limit' is applied last, as a row cap, not a column filter.
    '''
    mask = None
    for key, value in fields.items():
        if key == "limit" or key not in field_map:
            continue
        columns = field_map[key]
        if isinstance(columns, str):
            columns = [columns]
        columns = [c for c in columns if c in df.columns]
        if not columns:
            continue

        field_mask = None
        for col in columns:
            col_mask = df[col].astype(str).str.contains(str(value), case=False, na=False)
            field_mask = col_mask if field_mask is None else (field_mask | col_mask)

        mask = field_mask if mask is None else (mask & field_mask)

    if mask is not None:
        df = df[mask]

    if "limit" in fields:
        try:
            df = df.head(int(fields["limit"]))
        except (ValueError, TypeError):
            pass

    return df


def walk_hpc_catalog(entry, workspace_folder, fields, schema_crosswalk, log_fn=print, depth=0, max_depth=10):
    '''
    Recursively walks an HPC-registered catalog chain: pulls entry['path'], and if it's a CSV
    (same schema as federate_datasets.py's repo CSVs), recurses into every row as a child entry.
    If it's a terminal database file, its tables/columns are matched against the known schemas in
    schema_crosswalk (table name + a required column) to find its primary table; if no schema
    matches, falls back to a resources/records guess with a warning and no filtering. The primary
    table's rows are filtered against fields (keyword/author/etc.) using that schema's field_map
    (see filter_by_field_map), then become the primary result; every other table loads as a
    secondary table, unfiltered. No further recursion happens inside that terminal database.

    Args:
        entry: dict with location_type, location, path, submitter_name.
        workspace_folder: directory to pull downloaded files into.
        fields: parsed query fields (keyword/author/title/doi/id/limit).
        schema_crosswalk: dict of {schema_name: {"signature": {"table", "required_column"},
            "primary_table", "field_map"}} - e.g. loaded from hpc_schema_crosswalk.json.
        log_fn: callable(str) for progress/warning messages. Defaults to print; pass a no-op
            lambda to silence.
        depth, max_depth: recursion guard against runaway catalog chains.

    Returns:
        (resources_df_or_None, {table_name: df}).
    '''
    if depth >= max_depth:
        log_fn(f"scout_fetch WARNING: HPC catalog chain exceeded max depth ({max_depth}) at "
               f"{entry['location_type']}:{entry['location']}:{entry['path']}. Skipping.")
        return None, {}

    from dsi.utils.federated.federate_datasets import pull_data
    try:
        db_info, _ = pull_data(
            location_type=entry["location_type"],
            location=entry["location"],
            path=entry["path"],
            abs_path_workspace_folder=workspace_folder,
            username="",
            internal_use=True,
        )
    except Exception as e:
        log_fn(f"scout_fetch WARNING: failed to pull {entry['location_type']}:{entry['location']}:{entry['path']}: {e}")
        return None, {}

    local_path = db_info.get("local_path") if db_info else None
    if not local_path or not os.path.exists(local_path):
        log_fn(f"scout_fetch WARNING: could not retrieve {entry['location_type']}:{entry['location']}:{entry['path']}.")
        return None, {}

    if str(local_path).lower().endswith(".csv"):
        try:
            rows = csv_to_list_of_dicts(local_path)
        except Exception as e:
            log_fn(f"scout_fetch WARNING: could not parse catalog CSV {local_path}: {e}")
            return None, {}

        combined_resources = None
        combined_secondary = {}
        for row in rows:
            child_entry = {
                "location_type": row.get("location_type", ""),
                "location": row.get("location", ""),
                "path": row.get("path", ""),
                "submitter_name": row.get("submitter_name", ""),
            }
            child_resources, child_secondary = walk_hpc_catalog(
                child_entry, workspace_folder, fields, schema_crosswalk, log_fn=log_fn,
                depth=depth + 1, max_depth=max_depth)

            if child_resources is not None and not child_resources.empty:
                if combined_resources is None:
                    combined_resources = child_resources
                else:
                    all_cols = list(dict.fromkeys(list(combined_resources.columns) + list(child_resources.columns)))
                    combined_resources = pd.concat(
                        [combined_resources.reindex(columns=all_cols), child_resources.reindex(columns=all_cols)],
                        ignore_index=True)

            for dest_name, df in child_secondary.items():
                if dest_name in combined_secondary:
                    prev = combined_secondary[dest_name]
                    all_cols = list(dict.fromkeys(list(prev.columns) + list(df.columns)))
                    combined_secondary[dest_name] = pd.concat(
                        [prev.reindex(columns=all_cols), df.reindex(columns=all_cols)], ignore_index=True)
                else:
                    combined_secondary[dest_name] = df

        return combined_resources, combined_secondary

    # Terminal database file
    try:
        d = DSI(filename=local_path, backend_name="Sqlite", silence_messages=True)
    except Exception as e:
        log_fn(f"scout_fetch WARNING: could not open {local_path} as a database: {e}")
        return None, {}

    try:
        table_names = d.list(collection=True)
    except Exception as e:
        log_fn(f"scout_fetch WARNING: {local_path} has no usable tables: {e}")
        d.close()
        return None, {}

    # Match against known HPC catalog schemas (table name + a required column present in it).
    # First registered schema that matches wins.
    primary_table_name = None
    matched_field_map = None
    for schema in schema_crosswalk.values():
        sig = schema["signature"]
        if sig["table"] not in table_names:
            continue
        try:
            sig_cols = d.get_table(sig["table"], collection=True).columns
        except Exception:
            continue
        if sig["required_column"] in sig_cols:
            primary_table_name = schema["primary_table"]
            matched_field_map = schema.get("field_map", {})
            break

    if primary_table_name is None:
        log_fn(f"scout_fetch WARNING: no registered HPC schema matched tables in {local_path}; "
               f"falling back to a resources/records guess with no field mapping.")
        # Prefer "resources" over "records" if a catalog happens to have both.
        primary_table_name = next((t for t in table_names if t.lower() == "resources"), None)
        if primary_table_name is None:
            primary_table_name = next((t for t in table_names if t.lower() == "records"), None)

    resources_df = None
    secondary = {}
    for table_name in table_names:
        try:
            df = d.get_table(table_name, collection=True)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df = df.copy()

        if table_name == primary_table_name:
            if matched_field_map:
                df = filter_by_field_map(df, matched_field_map, fields)
            resources_df = df
        else:
            parent_col = next((c for c in df.columns if c.lower() in ("parent_id", "resource_id", "dataset_id")), None)
            if parent_col:
                df.insert(0, "fed_parent_id", df[parent_col])
            secondary[table_name] = df

    d.close()
    return resources_df, secondary
