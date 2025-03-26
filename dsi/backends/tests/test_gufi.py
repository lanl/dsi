from dsi.backends.gufi import Gufi

isVerbose = False


def test_artifact_query():
    dbpath = "db.db"
    index = "gufi_indexes"
    prefix = "/usr/local/bin"
    table = "sample"
    column = "sample_col"
    store = Gufi(prefix, index, dbpath, table, column, isVerbose)
    sqlstr = "select * from dsi_entries"
    rows = store.query_artifacts(sqlstr)
    store.close()
    assert len(rows) > 0
