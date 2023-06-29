import git

from dsi.drivers.sqlite import Sqlite, DataType

isVerbose = True


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_wildfire_data_sql_artifact():
    dbpath = "wildfire.db"
    store = Sqlite(dbpath)
    store.close()
    # No error implies success
    assert True


def test_wildfire_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'), 'dsi/data/wildfiredata.csv'])
    dbpath = "wildfire.db"
    store = Sqlite(dbpath)
    store.put_artifacts_csv(csvpath, "simulation", isVerbose=isVerbose)
    store.close()
    # No error implies success
    assert True


def test_yosemite_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'), 'dsi/data/yosemite5.csv'])
    dbpath = "yosemite.db"
    store = Sqlite(dbpath)
    store.put_artifacts_csv(csvpath, "vision", isVerbose=isVerbose)
    store.close()
    # No error implies success
    assert True


def test_artifact_query():
    dbpath = "wildfire.db"
    store = Sqlite(dbpath)
    _ = store.get_artifact_list(isVerbose=isVerbose)
    data_type = DataType()
    data_type.name = "simulation"
    result = store.sqlquery("SELECT *, MAX(wind_speed) AS max_windspeed FROM " +
                            str(data_type.name) + " GROUP BY safe_unsafe_fire_behavior")
    store.export_csv(result, "query.csv")
    store.close()
    # No error implies success
    assert True
