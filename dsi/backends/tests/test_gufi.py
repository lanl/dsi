from dsi.backends.gufi import Gufi

def test_artifact_query():
<<<<<<< Updated upstream
    gufi_index_path = "/opt/example-collection"
=======
#    gufi_index_path = "/opt/example-collection"
    gufi_index_path = "/opt/example-collection/tests/images"
>>>>>>> Stashed changes
    gufi_prefix = "/home/hgreenburg/GUFI/build/"
    dsi_table_name = "wfdata"
    dsi_file_path_column = "LOCAL_PATH"
    dsi_columns = ["sim_id", "wind_speed"]
    gufi_columns = ["fullpath", "size", "mtime"]
<<<<<<< Updated upstream
    collection_name = "collection1234"
=======
    collection_name = "collection"
>>>>>>> Stashed changes
    gufi_tag_tool_path = "/home/hgreenburg/gufi-dsi-tag-user-namespace/target/release/gufi-dsi-tag"
    from dsi.core import Sync
    s = Sync("collection")
    s.gufi_query_index(gufi_prefix, gufi_index_path, dsi_table_name, dsi_file_path_column, dsi_columns,
                       gufi_columns, collection_name, gufi_tag_tool_path, isVerbose=True)
    #assert len(rows) > 0

test_artifact_query()
