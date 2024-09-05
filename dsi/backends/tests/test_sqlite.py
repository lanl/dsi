import git
from collections import OrderedDict

from dsi.backends.sqlite import Sqlite, DataType

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

def test_wildfiredata_artifact_put():
   valid_middleware_datastructure = OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})
   dbpath = 'test_wildfiredata_artifact.sqlite_data'
   store = Sqlite(dbpath)
   store.put_artifacts(valid_middleware_datastructure)
   store.close()
   # No error implies success
   assert True

def test_wildfiredata_artifact_put_t():
   valid_middleware_datastructure = OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})
   dbpath = 'test_wildfiredata_artifact.sqlite_data'
   store = Sqlite(dbpath)
   store.put_artifacts_t(valid_middleware_datastructure, tableName="Wildfire")
   store.close()
   # No error implies success
   assert True

#Data from: https://microsoftedge.github.io/Demos/json-dummy-data/64KB.json
def test_jsondata_artifact_put():
   jsonpath = '/'.join([get_git_root('.'), 'dsi/data/64KB.json'])
   dbpath = "jsondata.db"
   store = Sqlite(dbpath)
   store.put_artifacts_json(jsonpath, tname="JSONData")
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
    store.export_csv(result, "TABLENAME", "query.csv")
    store.close()
    # No error implies success
    assert True


test_jsondata_artifact_put()

def test_yaml_reader():
    reader = Sqlite("yaml-test.db")
    reader.yamlToSqlite("../../../examples/data/schema.yml", "yaml-test", deleteSql=False)
    subprocess.run(["diff", "../../../examples/data/compare-schema.sql", "yaml-test.sql"], stdout=open("compare_sql.txt", "w"))
    file_size = os.path.getsize("compare_sql.txt")
    os.remove("compare_sql.txt")
    os.remove("yaml-test.sql")
    os.remove("yaml-test.db")

    assert file_size == 0 #difference between sql files should be 0 characters

def test_toml_reader():
    reader = Sqlite("toml-test.db")
    reader.tomlToSqlite("../../../examples/data/schema.toml", "toml-test", deleteSql=False)
    subprocess.run(["diff", "../../../examples/data/compare-schema.sql", "toml-test.sql"], stdout=open("compare_sql.txt", "w"))
    file_size = os.path.getsize("compare_sql.txt")
    os.remove("compare_sql.txt")
    os.remove("toml-test.sql")
    os.remove("toml-test.db")

    assert file_size == 0 #difference between sql files should be 0 characters