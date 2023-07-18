from dsi.drivers.gufi import Gufi
from dsi.permissions.permissions import PermissionsManager

isVerbose = False


def test_artifact_query():
    dbpath = "db.db"
    index = "gufi_indexes"
    prefix = "/usr/local/bin"
    table = "sample"
    column = "sample_col"
    mock_pm = PermissionsManager()
    store = Gufi(prefix, index, dbpath, table, column,
                 isVerbose, perms_manager=mock_pm)
    sqlstr = "select * from dsi_entries"
    rows = store.get_artifacts(sqlstr)
    store.close()
    assert len(rows) > 0
