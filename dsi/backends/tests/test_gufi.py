
def test_artifact_query():
    gufi_index_path = "/search/db_name"
    gufi_prefix = "/usr/projects/systems/gufi/"
    dsi_table_name = "metadata_uuid"
    db_path = "/db_name.db"
    dsi_columns = ["metric1", "metric2", "time"]
    gufi_columns = ["fullpath", "size", "mtime"]
    collection_name = "DATA_UUID"
    dsi_column_names = ",".join(dsi_columns)
    custom_query = f"""
    SELECT uview.*, {dsi_column_names} FROM uview JOIN {collection_name}.{dsi_table_name} ON uview.uuid ==
    {collection_name}.{dsi_table_name}.uuid where metric2 > 1000
    """

    from dsi.sync import Sync
    s = Sync()
    rows = s.gufi_query_index(gufi_prefix, gufi_index_path, db_path, dsi_table_name, dsi_columns,
                       gufi_columns, collection_name, custom_query, isVerbose=True)
    assert len(rows) > 0
