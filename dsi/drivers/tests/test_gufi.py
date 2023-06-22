from dsi.drivers.gufi import Gufi, DataType

isVerbose=False

def test_query():    
    #Query everything
    #store.close()
    pass


def test_artifact_query():
    dbpath = "db.db"
    index = "gufi_indexes"
    prefix = "/usr/local/gufi"
    table = "sample"
    column = "sample_col"
    store = Gufi(prefix, index, dbpath, table, column, isVerbose)
    sqlstr = "select * from dsi_entries"
    rows = store.get_artifacts(sqlstr)
    print( rows )

    store.close()
    print("Done")

test_artifact_query()
