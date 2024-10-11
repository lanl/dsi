from dsi.backends.sqlite import Sqlite, DataType

def create_sqlite_from_csv(sqldb_name, csv_filepath, verbose=False):
    """
    Compute a sqlite database from a csv file and saves it at <sqldb_name>.db
    Only one table is created

    Args:
        sqldb_name (str): name of the sqlite db and table to create
        csv_filepath (str): Path of CSV file
        verbose (bool): verbose or not, default is False

    Returns:
        Nothing
    """
    dbpath = sqldb_name + '.db'
    store = Sqlite(dbpath)
    store.put_artifacts_csv(csv_filepath, sqldb_name, isVerbose=verbose)
    store.close()