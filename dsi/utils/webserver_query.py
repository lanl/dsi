from dsi.dsi import DSI


def query_webserver(entry, fields, webserver_crosswalk, log_fn=print):
    '''
    Queries a single registered webserver backend entry using fields (keyword/author/etc.),
    translated through webserver_crosswalk's field_map (e.g. loaded from webserver_crosswalk.json).

    Args:
        entry: dict with "backend_name" (e.g. "OSTI").
        fields: parsed query fields (keyword/author/title/doi/id/limit).
        webserver_crosswalk: dict of {backend_name: {"field_map", "primary_table", "pk", "fk"}}.
        log_fn: callable(str) for progress/warning messages. Defaults to print; pass a no-op
            lambda to silence.

    Returns:
        (primary_df_or_None, {table_name: df}) - same shape as catalog_walk.walk_hpc_catalog, so
        callers can treat both source kinds uniformly.
    '''
    backend_name = entry["backend_name"]
    backend_crosswalk = webserver_crosswalk.get(backend_name, {})
    field_map = backend_crosswalk.get("field_map", {})
    params = {field_map[k]: v for k, v in fields.items() if k in field_map}
    if not params:
        log_fn(f"Skipping '{backend_name}': none of the given fields are supported by this backend.")
        return None, {}

    try:
        d = DSI(backend_name=backend_name, params=params, silence_messages=True)
    except Exception as e:
        log_fn(f"'{backend_name}' query failed: {e}")
        return None, {}

    try:
        table_names = d.list(collection=True)
    except Exception as e:
        log_fn(f"'{backend_name}' returned no usable tables: {e}")
        d.close()
        return None, {}

    primary_name = backend_crosswalk.get("primary_table")
    pk_col = backend_crosswalk.get("pk")
    fk_col = backend_crosswalk.get("fk")  # only set for backends with a shared, multi-parent secondary table

    primary_df = None
    secondary = {}
    backend_result_count = 0
    for table_name in table_names:
        try:
            df = d.get_table(table_name, collection=True)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df = df.copy()

        if table_name == primary_name:
            df.insert(0, "fed_source", backend_name)
            if pk_col and pk_col in df.columns:
                df.insert(1, "fed_id", df[pk_col].astype(str).map(lambda v: f"{backend_name}:{v}"))
            else:
                df.insert(1, "fed_id", None)
            backend_result_count = len(df)
            primary_df = df
        else:
            # Only tag fed_parent_id for backends whose secondary table mixes rows from multiple
            # parent records (NDP/RCSBPDB/Zenodo "resources"). Oceans11's Tier-2 tables are already
            # dedicated to one record each (named "<t2db_name>_<table>") - no FK needed, nothing to add.
            if fk_col and fk_col in df.columns:
                df.insert(0, "fed_parent_id", df[fk_col].astype(str).map(lambda v: f"{backend_name}:{v}"))
            secondary[table_name] = df

    d.close()
    log_fn(f"'{backend_name}' returned {backend_result_count} primary result(s).")
    return primary_df, secondary
